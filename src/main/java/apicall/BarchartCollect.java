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
import java.util.stream.Collectors;

import static Util.ProxyManager.getPublicIP;

public class BarchartCollect {

    public static void main(String[] args) throws Exception {
        // ---- Usage guide ----
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

        ProxyManager proxyManager = new ProxyManager(config, LogUtil::log);

        List<String> tickers;
        if (args.length >= 1) {
            String arg = args[0].trim().toLowerCase();
            if ("priority".equals(arg)) {
                LogUtil.log("Running in PRIORITY mode (only Priority=1 symbols)");
                tickers = loadTickers(config, true);
            } else {
                LogUtil.log("Running in single-symbol mode for: " + args[0]);
                tickers = Collections.singletonList(args[0].toUpperCase());
            }
        } else {
            tickers = loadTickers(config, false);
            if (tickers.isEmpty()) {
                LogUtil.log("No tickers found in symbol_list.");
                return;
            }
        }

        int threadCount = getThreadCount(args);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (String ticker : tickers) {
            executor.submit(() -> {
                try {
                    // Step 1: expiration-level data
                    java.sql.Date updateDate = new java.sql.Date(System.currentTimeMillis());
                    processTicker(ticker, config, proxyManager);

                    // Step 2: strike-level data (all expirations found in DB)
                    BarchartOptionChainCollect.collectOptionChains(ticker, updateDate, config, proxyManager);

                    // Step 3: extra volatility fetch
                    BarchartHtmlFetcher.fetchAndStoreVolatilityData(ticker, config, proxyManager);

                    LogUtil.log("Data inserted/updated for: " + ticker);
                } catch (Exception e) {
                    LogUtil.log("Error processing ticker " + ticker + ": " + e.getMessage());
                }
            });

        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);

        LogUtil.log("All tickers processed.");
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
    }

    private static List<String> loadTickers(ConfigLoader config, boolean priorityOnly) throws Exception {
        List<String> tickers = new ArrayList<>();
        String sql = priorityOnly
                ? "SELECT ticker FROM symbol_list WHERE Priority = 1"
                : "SELECT ticker FROM symbol_list";

        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                tickers.add(rs.getString("ticker"));
            }
        }
        return tickers;
    }

    private static void processTicker(String ticker, ConfigLoader config, ProxyManager proxyManager) {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                proxyUsed = proxyManager.getNextProxy();
                HttpClient client = HttpHelper.createHttpClientForSymbol(proxyUsed);
                String ip = getPublicIP(client);

                LogUtil.log("Processing " + ticker + " using proxy: " + proxyUsed + ", public IP detected: " + ip);

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
                        + "putCallOpenInterestRatio,averageVolatility,symbolCode,symbolType,lastPrice,dailyLastPrice"
                        + "&symbol=" + ticker
                        + "&page=1&limit=100&raw=1";

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

                if (apiResponse.statusCode() == 200) {
                    saveToDatabase(apiResponse.body(), config, ticker);
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

    private static void saveToDatabase(String jsonResponse, ConfigLoader config, String tickerFromDb) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);
        JsonNode dataArray = root.path("data");

        ZoneId nyZone = ZoneId.of("America/New_York");
        ZonedDateTime nyNow = ZonedDateTime.now(nyZone);
        java.sql.Date updateDate = java.sql.Date.valueOf(nyNow.toLocalDate());
        java.sql.Time updateTime = java.sql.Time.valueOf(nyNow.toLocalTime().withNano(0));

        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword())) {
            String sql = "INSERT INTO market_data(" +
                    "symbol, Cycle_Range, expiration_date, update_date, update_time, " +
                    "DTE, Put_Vol, Call_Vol, Total_Vol, " +
                    "Put_or_Call_Vol, Put_OI, Call_OI, Total_OI, " +
                    "Put_or_Call_OI, IV) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "Cycle_Range=VALUES(Cycle_Range), update_time=VALUES(update_time), " +
                    "DTE=VALUES(DTE), Put_Vol=VALUES(Put_Vol), Call_Vol=VALUES(Call_Vol), " +
                    "Total_Vol=VALUES(Total_Vol), Put_or_Call_Vol=VALUES(Put_or_Call_Vol), " +
                    "Put_OI=VALUES(Put_OI), Call_OI=VALUES(Call_OI), Total_OI=VALUES(Total_OI), " +
                    "Put_or_Call_OI=VALUES(Put_or_Call_OI), IV=VALUES(IV)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (JsonNode item : dataArray) {
                    String expirationDate = item.path("expirationDate").asText();
                    String expirationType = item.path("expirationType").asText("").trim().toLowerCase();

                    if ("n/a".equals(expirationType) || expirationType.isEmpty()) {
                        LogUtil.log("Skipping expirationType n/a for symbol: " + tickerFromDb);
                        continue;
                    }

                    char typeChar = expirationType.equals("weekly") ? 'w' :
                            expirationType.equals("monthly") ? 'm' : 'x';
                    if (typeChar == 'x') {
                        LogUtil.log("Skipping unknown expirationType '" + expirationType + "' for symbol: " + tickerFromDb);
                        continue;
                    }

                    String formattedDate = LocalDate.parse(expirationDate, DateTimeFormatter.ofPattern("MM/dd/yy"))
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String expirationKey = formattedDate + "-" + typeChar;

                    int daysToExpiration = NumberParser.parseIntSafe(item.path("daysToExpiration").asText());
                    int putVolume = NumberParser.parseIntSafe(item.path("putVolume").asText());
                    int callVolume = NumberParser.parseIntSafe(item.path("callVolume").asText());
                    int totalVolume = NumberParser.parseIntSafe(item.path("totalVolume").asText());
                    double putCallVolumeRatio = NumberParser.parseDoubleSafe(item.path("putCallVolumeRatio").asText());
                    int putOpenInterest = NumberParser.parseIntSafe(item.path("putOpenInterest").asText());
                    int callOpenInterest = NumberParser.parseIntSafe(item.path("callOpenInterest").asText());
                    int totalOpenInterest = NumberParser.parseIntSafe(item.path("totalOpenInterest").asText());
                    double putCallOpenInterestRatio = NumberParser.parseDoubleSafe(item.path("putCallOpenInterestRatio").asText());
                    double impliedVolatility = NumberParser.parseVolatility(item.path("averageVolatility").asText());

                    stmt.setString(1, tickerFromDb);
                    stmt.setString(2, CycleHelper.getCycleRange());
                    stmt.setString(3, expirationKey);
                    stmt.setDate(4, updateDate);
                    stmt.setTime(5, updateTime);
                    stmt.setInt(6, daysToExpiration);
                    stmt.setInt(7, putVolume);
                    stmt.setInt(8, callVolume);
                    stmt.setInt(9, totalVolume);
                    stmt.setDouble(10, putCallVolumeRatio);
                    stmt.setInt(11, putOpenInterest);
                    stmt.setInt(12, callOpenInterest);
                    stmt.setInt(13, totalOpenInterest);
                    stmt.setDouble(14, putCallOpenInterestRatio);
                    stmt.setDouble(15, impliedVolatility);

                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }
    }
}
