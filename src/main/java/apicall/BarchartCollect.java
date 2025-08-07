package apicall;

import Util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static Util.ProxyManager.getPublicIP;

public class BarchartCollect {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help"))) {
            System.out.println("Usage:");
            System.out.println("  java -cp yourJar.jar apicall.BarchartCollect");
            System.out.println("    -> Processes all tickers from symbol_list database table");
            System.out.println("  java -cp yourJar.jar apicall.BarchartCollect <SYMBOL>");
            System.out.println("    -> Processes only the specified symbol (e.g., AAPL)");
            System.out.println("  java -cp yourJar.jar apicall.BarchartCollect priority");
            System.out.println("    -> Processes only symbols with Priority=1");
            System.out.println("  java -cp yourJar.jar apicall.BarchartCollect priority 8");
            System.out.println("    -> Processes only Priority=1 symbols using 8 threads");
            return;
        }

        ConfigLoader config = new ConfigLoader();
        initLogFile(config);
        int threadCount = getThreadCount(args);
        ConnectionPool.init(config, threadCount);
        ProxyManager proxyManager = new ProxyManager(config, LogUtil::log);

        List<String> tickers;
        if (args.length >= 1) {
            String arg = args[0].trim().toLowerCase();
            if ("priority".equals(arg)) {
                LogUtil.log("Running in PRIORITY mode (only Priority=1 symbols)");
                tickers = loadTickers(true);
            } else {
                LogUtil.log("Running in single-symbol mode for: " + args[0]);
                tickers = Collections.singletonList(args[0].toUpperCase());
            }
        } else {
            tickers = loadTickers(false);
            if (tickers.isEmpty()) {
                LogUtil.log("No tickers found in symbol_list.");
                return;
            }
        }


        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        AtomicInteger completedCount = new AtomicInteger(0);
        long startTime = System.nanoTime();

        LogUtil.log("🚀 Starting processing of " + tickers.size() + " symbols using " + threadCount + " threads...");

        // Periodic logger for progress
        ScheduledExecutorService progressLogger = Executors.newSingleThreadScheduledExecutor();
        progressLogger.scheduleAtFixedRate(() -> {
            int done = completedCount.get();
            long elapsedNano = System.nanoTime() - startTime;
            double elapsedMinutes = elapsedNano / 1_000_000_000.0 / 60.0;
            double speed = elapsedMinutes > 0 ? done / elapsedMinutes : 0.0;

            LogUtil.log("⏳ Progress: " + done + "/" + tickers.size() +
                    " completed | " + String.format("%.2f", speed) + " symbols/min");

            // Stop logging when all are completed
            if (done >= tickers.size()) {
                LogUtil.log("🛑 Progress logging stopping — all tickers processed.");
                progressLogger.shutdown();
            }
        }, 60, 60, TimeUnit.SECONDS); // Initial delay: 60s, Interval: 60s

        // Submit tasks
        for (String ticker : tickers) {
            executor.submit(() -> {
                try {
                    java.sql.Date updateDate = new java.sql.Date(System.currentTimeMillis());
                    processTicker(ticker, proxyManager);

                    BarchartOptionChainCollect.collectOptionChains(ticker, updateDate, config, proxyManager);
                    BarchartHtmlFetcher.fetchAndStoreVolatilityData(ticker, config, proxyManager);

                    LogUtil.log("✅ Completed processing for: " + ticker);
                    completedCount.incrementAndGet();
                } catch (Exception e) {
                    LogUtil.log("❌ Error processing ticker " + ticker + ": " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            LogUtil.log("⚠️ Executor interrupted while waiting for tasks to finish.");
            Thread.currentThread().interrupt(); // Restore interrupt status
        }

        // Final timing and summary
        long endTime = System.nanoTime();
        double durationMinutes = (endTime - startTime) / 1_000_000_000.0 / 60.0;
        int totalDone = completedCount.get();
        double finalSpeed = totalDone / durationMinutes;

        LogUtil.log("✅ All tickers processed.");
        LogUtil.log("⏱️  Total time: " + String.format("%.2f", durationMinutes) + " minutes");
        LogUtil.log("⚡ Final speed: " + String.format("%.2f", finalSpeed) + " symbols per minute");

        ConnectionPool.close();
    }



    private static int getThreadCount(String[] args) {
        int threadCount;
        if (args.length >= 2) {
            try {
                threadCount = Integer.parseInt(args[1]);
                LogUtil.log("Thread count set from argument: " + threadCount);
            } catch (NumberFormatException e) {
                LogUtil.log("Invalid thread count argument, using default.");
                threadCount = Runtime.getRuntime().availableProcessors();
            }
        } else {
            threadCount = (int) Math.round(Runtime.getRuntime().availableProcessors() / 1.3);
            LogUtil.log("Thread count set to available processors: " + threadCount);
        }
        return threadCount;
    }

    private static void initLogFile(ConfigLoader config) throws Exception {
        String logDir = config.getProperty("log.path", ".");
        Path logDirPath = Paths.get(logDir);
        if (!Files.exists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }

        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").format(LocalDateTime.now());
        Path logFile = logDirPath.resolve("barchart_" + timestamp + ".log");

        LogUtil.init(logFile);

        // 🔁 Tell Logback to use the same file
        System.setProperty("logFile", logFile.toString());
    }

    private static List<String> loadTickers( boolean priorityOnly) throws Exception {
        List<String> tickers = new ArrayList<>();
        String sql = priorityOnly
                ? "SELECT ticker FROM symbol_list WHERE Priority = 1"
                : "SELECT ticker FROM symbol_list";

        try ( Connection conn = ConnectionPool.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                tickers.add(rs.getString("ticker"));
            }
        }
        return tickers;
    }

    private static void processTicker(String ticker, ProxyManager proxyManager) {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                proxyUsed = proxyManager.getNextProxy();
                HttpClient client = HttpHelper.createHttpClientForSymbol(proxyUsed);
                String ip = getPublicIP(client);

//                LogUtil.log("Processing " + ticker + " using proxy: " + proxyUsed + ", public IP detected: " + ip);

                String pageUrl = "https://www.barchart.com/stocks/quotes/" + ticker + "/put-call-ratios";
                HttpRequest pageRequest = HttpRequest.newBuilder()
                        .uri(URI.create(pageUrl))
                        .GET()
                        .timeout(java.time.Duration.ofSeconds(30))
                        .header("user-agent", UserAgentProvider.getRandomUserAgent())
                        .header("accept-language", "en-US,en;q=0.9")
                        .header("cache-control", "no-cache")
                        .header("accept", "text/html")
                        .build();

                HttpResponse<String> pageResponse = client.send(pageRequest, HttpResponse.BodyHandlers.ofString());
                List<String> setCookies = pageResponse.headers().allValues("set-cookie");
                String cookieHeader = setCookies.stream()
                        .map(c -> c.split(";", 2)[0])
                        .collect(Collectors.joining("; ")) + "; bcFreeUserPageView=0";
                String xsrfToken = HttpHelper.extractXsrfFromCookies(setCookies);

                String apiUrl = "https://www.barchart.com/proxies/core-api/v1/options-expirations/get"
                        + "?fields=expirationDate,expirationType,daysToExpiration,putVolume,callVolume,totalVolume,"
                        + "putCallVolumeRatio,putOpenInterest,callOpenInterest,totalOpenInterest,"
                        + "putCallOpenInterestRatio,averageVolatility,symbolCode,symbolType,lastPrice,dailyLastPrice,"
                        + "baseLastPrice,impliedMove,impliedMovePercent,baseUpperPrice,baseLowerPrice,"
                        + "&symbol=" + ticker + "&page=1&limit=100&raw=1";

                HttpRequest apiRequest = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .GET()
                        .timeout(java.time.Duration.ofSeconds(30))
                        .header("accept", "application/json")
                        .header("cookie", cookieHeader)
                        .header("x-xsrf-token", xsrfToken)
                        .header("referer", pageUrl)
                        .build();

                HttpResponse<String> apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());

//                LogUtil.log("API Response for " + ticker + ": " + apiResponse.body());

                if (apiResponse.statusCode() == 200) {
                    saveToDatabase(apiResponse.body(), ticker);
                    break;
                } else {
                    LogUtil.log("Failed API for " + ticker + ": " + apiResponse.statusCode());
                }
            } catch (Exception e) {
                LogUtil.log("Error processing ticker " + ticker + " (attempt " + attempt + "): " + e.getMessage());
                if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
            }
            try { Thread.sleep(2000 * attempt); } catch (InterruptedException ignored) {}
        }
    }

    private static void saveToDatabase(String jsonResponse, String tickerFromDb) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);
        JsonNode dataArray = root.path("data");

        ZoneId nyZone = ZoneId.of("America/New_York");
        ZonedDateTime nyNow = ZonedDateTime.now(nyZone);
        java.sql.Date updateDate = java.sql.Date.valueOf(nyNow.toLocalDate());
        java.sql.Time updateTime = java.sql.Time.valueOf(nyNow.toLocalTime().withNano(0));
        int batchSize = 200;
        int count = 0;        // Row counter for batch size
        try (Connection conn = ConnectionPool.getConnection()) {
            String sql = "INSERT INTO market_data(" +
                    "symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time, " +
                    "DTE, Put_Vol, Call_Vol, Total_Vol, Put_or_Call_Vol, Put_OI, Call_OI, Total_OI, " +
                    "Put_or_Call_OI, IV, Base_LastPrice, Implied_Move, Implied_Move_Percent, Base_Upper_Price, Base_Lower_Price) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "Cycle_Range=VALUES(Cycle_Range), update_time=VALUES(update_time), expiration_type=VALUES(expiration_type), " +
                    "DTE=VALUES(DTE), Put_Vol=VALUES(Put_Vol), Call_Vol=VALUES(Call_Vol), Total_Vol=VALUES(Total_Vol), " +
                    "Put_or_Call_Vol=VALUES(Put_or_Call_Vol), Put_OI=VALUES(Put_OI), Call_OI=VALUES(Call_OI), " +
                    "Total_OI=VALUES(Total_OI), Put_or_Call_OI=VALUES(Put_or_Call_OI), IV=VALUES(IV), " +
                    "Base_LastPrice=VALUES(Base_LastPrice), Implied_Move=VALUES(Implied_Move), " +
                    "Implied_Move_Percent=VALUES(Implied_Move_Percent), Base_Upper_Price=VALUES(Base_Upper_Price), " +
                    "Base_Lower_Price=VALUES(Base_Lower_Price)";


            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                DateTimeFormatter inputFmt = DateTimeFormatter.ofPattern("MM/dd/yy");
                DateTimeFormatter outputFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");

                for (JsonNode item : dataArray) {
                    String expirationDate = item.path("expirationDate").asText();
                    String expirationType = item.path("expirationType").asText("").toLowerCase();

                    if ("n/a".equals(expirationType) || expirationType.isEmpty()) {
                        LogUtil.log("Skipping expirationType n/a for symbol: " + tickerFromDb);
                        continue;
                    }
                    String formattedDate = LocalDate.parse(expirationDate, inputFmt).format(outputFmt);

                    stmt.setString(1, tickerFromDb);
                    stmt.setString(2, CycleHelper.getCycleRange());
                    stmt.setString(3, formattedDate);       // save raw date
                    stmt.setString(4, expirationType);      // weekly or monthly
                    stmt.setDate(5, updateDate);
                    stmt.setTime(6, updateTime);
                    stmt.setInt(7, NumberParser.parseIntSafe(item.path("daysToExpiration").asText()));
                    stmt.setInt(8, NumberParser.parseIntSafe(item.path("putVolume").asText()));
                    stmt.setInt(9, NumberParser.parseIntSafe(item.path("callVolume").asText()));
                    stmt.setInt(10, NumberParser.parseIntSafe(item.path("totalVolume").asText()));
                    stmt.setDouble(11, NumberParser.parseDoubleSafe(item.path("putCallVolumeRatio").asText()));
                    stmt.setInt(12, NumberParser.parseIntSafe(item.path("putOpenInterest").asText()));
                    stmt.setInt(13, NumberParser.parseIntSafe(item.path("callOpenInterest").asText()));
                    stmt.setInt(14, NumberParser.parseIntSafe(item.path("totalOpenInterest").asText()));
                    stmt.setDouble(15, NumberParser.parseDoubleSafe(item.path("putCallOpenInterestRatio").asText()));
                    stmt.setDouble(16, NumberParser.parseVolatility(item.path("averageVolatility").asText()));
                    stmt.setDouble(17, NumberParser.parseDoubleSafe(item.path("baseLastPrice").asText()));
                    stmt.setDouble(18, NumberParser.parseDoubleSafe(item.path("impliedMove").asText()));

                    // --- FIX: Use raw.impliedMovePercent ---
                    double impliedMovePercent = 0.0;
                    JsonNode rawNode = item.path("raw");
                    if (rawNode != null && rawNode.has("impliedMovePercent")) {
                        impliedMovePercent = rawNode.path("impliedMovePercent").asDouble();
                    }
                    stmt.setDouble(19, impliedMovePercent);

                    stmt.setDouble(20, NumberParser.parseDoubleSafe(item.path("baseUpperPrice").asText()));
                    stmt.setDouble(21, NumberParser.parseDoubleSafe(item.path("baseLowerPrice").asText()));

                    stmt.addBatch();
                    count++;
                    if (count % batchSize == 0) {
                        stmt.executeBatch(); // Execute batch every `batchSize` rows
                        count = 0; // Reset the batch counter
                    }
                }
                if (count > 0) {
                    stmt.executeBatch(); // Execute any remaining rows in the batch
                }
            }
        }
    }
}
