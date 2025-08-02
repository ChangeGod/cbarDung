package apicall;

import Util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static Util.ProxyManager.getPublicIP;


public class BarchartCollect {

    private static Path logFile;
    private static final Object rateLimitLock = new Object();
    private static volatile long lastRateLimitWait = 0;

    public static void main(String[] args) throws Exception {
        // ---- Usage guide ----
        if (args.length > 0 && (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help"))) {
            System.out.println("Usage:");
            System.out.println("  java -cp yourJar.jar apicall.BarchartExpirationsCollect");
            System.out.println("    -> Processes all tickers from symbol_list database table");
            System.out.println("  java -cp yourJar.jar apicall.BarchartExpirationsCollect <SYMBOL>");
            System.out.println("    -> Processes only the specified symbol (e.g., AAPL)");
            System.out.println("  java -cp yourJar.jar apicall.BarchartExpirationsCollect priority");
            System.out.println("    -> Processes only symbols with Priority=1");
            System.out.println("  java -cp yourJar.jar apicall.BarchartExpirationsCollect priority 8");
            System.out.println("    -> Processes only Priority=1 symbols using 8 threads");
            return;
        }

        ConfigLoader config = new ConfigLoader();

        // ---- Prepare log file path ----
        String logDir = config.getProperty("log.path", ".");
        Path logDirPath = Paths.get(logDir);
        if (!Files.exists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }


        String modeSuffix = (args.length == 1 && args[0].equalsIgnoreCase("priority"))
                ? "_priority"
                : "_all";

        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").format(LocalDateTime.now());
        String logName = "barchart" + modeSuffix + "_" + timestamp + ".log";

        logFile = logDirPath.resolve(logName);


        // ---- Initialize ProxyManager with logger ----
        ProxyManager proxyManager = new ProxyManager(config, message -> log(message));

        List<String> tickers;

        if (args.length >= 1) {
            String arg = args[0].trim().toLowerCase();
            if ("priority".equals(arg)) {
                log("Running in PRIORITY mode (only Priority=1 symbols)");
                tickers = loadTickers(config, true);
            } else if (!"priority".equals(arg)) {
                log("Running in single-symbol mode for: " + args[0]);
                tickers = new ArrayList<>();
                tickers.add(args[0].toUpperCase());
            } else {
                tickers = loadTickers(config, false);
            }
        } else {
            tickers = loadTickers(config, false);
            if (tickers.isEmpty()) {
                log("No tickers found in symbol_list.");
                return;
            }
        }

        // ---- Thread count ----
        int threadCount;
        if (args.length >= 2) {
            try {
                threadCount = Integer.parseInt(args[1]);
                log("Thread count set from argument: " + threadCount);
            } catch (NumberFormatException e) {
                log("Invalid thread count argument, using default.");
                threadCount = Runtime.getRuntime().availableProcessors();
            }
        } else {
            threadCount = Runtime.getRuntime().availableProcessors();
            log("Thread count set to available processors: " + threadCount);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (String ticker : tickers) {
            executor.submit(() -> {
                processTicker(ticker, config, proxyManager);
                BarchartHtmlFetcher.fetchAndStoreVolatilityData(ticker, config, proxyManager);
                log("Data inserted/updated for: " + ticker);
            });
        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);

        log("All tickers processed.");
    }


    private static void handleRateLimit() {
        synchronized (rateLimitLock) {
            long now = System.currentTimeMillis();
            long elapsed = now - lastRateLimitWait;

            if (elapsed < 30000) {
                log("Rate limit recently handled, skipping additional wait.");
                return;
            }

            log("Global rate limit hit, waiting 30s before continuing...");
            lastRateLimitWait = now;
            try {
                Thread.sleep(30000); // <---- This now blocks all threads waiting here
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void processTicker(String ticker, ConfigLoader config, ProxyManager proxyManager) {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                proxyUsed = proxyManager.getNextProxy();
                HttpClient client = createHttpClientForSymbol(proxyUsed);
                String ip = getPublicIP(client);

                log("Processing " + ticker + " using proxy: " + proxyUsed + ", public IP detected: " + ip);

                // ---- ALWAYS fetch page -> get fresh cookies & XSRF token ----
                String pageUrl = "https://www.barchart.com/stocks/quotes/" + ticker + "/put-call-ratios";
                HttpRequest pageRequest = HttpRequest.newBuilder()
                        .uri(URI.create(pageUrl))
                        .GET()
                        .timeout(Duration.ofSeconds(30))
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
                String xsrfToken = extractXsrfFromCookies(setCookies);

                // ---- API Call ----
                String apiUrl = "https://www.barchart.com/proxies/core-api/v1/options-expirations/get"
                        + "?fields=" + URLEncoder.encode(
                        "expirationDate,expirationType,daysToExpiration,putVolume,callVolume,totalVolume," +
                                "putCallVolumeRatio,putOpenInterest,callOpenInterest,totalOpenInterest," +
                                "putCallOpenInterestRatio,averageVolatility,symbolCode,symbolType,lastPrice,dailyLastPrice",
                        StandardCharsets.UTF_8)
                        + "&symbol=" + ticker
                        + "&page=1&limit=100&raw=1";


                HttpRequest apiRequest = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .GET()
                        .timeout(Duration.ofSeconds(30))
                        .header("accept", "application/json")
                        .header("cookie", cookieHeader)
                        .header("x-xsrf-token", xsrfToken)
                        .header("referer", pageUrl)
                        .build();

                HttpResponse<String> apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());

                // ---- Log full API response ----
//                log("API response for " + ticker + ": " + apiResponse.body());

                // ---- Response Handling ----
                if (apiResponse.statusCode() == 200) {
                    saveToDatabase(apiResponse.body(), config, ticker);
//                    log("Data inserted/updated for: " + ticker);
                    break; // success
                } else if (apiResponse.statusCode() == 429) {
                    log("Rate limit hit for " + ticker);
                    handleRateLimit();
                } else if (apiResponse.statusCode() == 403) {
                    log("403 Forbidden for " + ticker + " - refreshing session & retrying");
                    proxyManager.markProxyBad(proxyUsed);
                } else if (apiResponse.statusCode() == 502) {
                    log("Tunnel failed for ticker " + ticker + " - will retry with next proxy.");
                    if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
                } else {
                    log("Failed API for " + ticker + ": " + apiResponse.statusCode());
                }

            } catch (Exception e) {
                log("Error processing ticker " + ticker + " (attempt " + attempt + "): " + e.getMessage());
                if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
            }

            try {
                Thread.sleep(2000 * attempt); // exponential backoff
            } catch (InterruptedException ignored) {}
        }
    }

    private static List<String> loadTickers(ConfigLoader config, boolean priorityOnly) throws Exception {
        List<String> tickers = new ArrayList<>();
        String sql = priorityOnly
                ? "SELECT ticker FROM symbol_list WHERE Priority = 1"
                : "SELECT ticker FROM symbol_list";

        try (Connection conn = DriverManager.getConnection(
                config.getDbUrl(), config.getDbUser(), config.getDbPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                tickers.add(rs.getString("ticker"));
            }
        }
        return tickers;
    }


    private static void saveToDatabase(String jsonResponse, ConfigLoader config, String tickerFromDb) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);
        JsonNode dataArray = root.path("data");

        ZoneId nyZone = ZoneId.of("America/New_York");
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("MM/dd/yy");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        try (Connection conn = DriverManager.getConnection(
                config.getDbUrl(), config.getDbUser(), config.getDbPassword())) {

            String sql = "INSERT INTO market_data(" +
                    "symbol, Cycle_Range, expiration_date, update_time, " +
                    "DTE, Put_Vol, Call_Vol, Total_Vol, " +
                    "Put_or_Call_Vol, Put_OI, Call_OI, Total_OI, " +
                    "Put_or_Call_OI, IV) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "Cycle_Range = VALUES(Cycle_Range), " +
                    "update_time = VALUES(update_time), " +
                    "DTE = VALUES(DTE), " +
                    "Put_Vol = VALUES(Put_Vol), " +
                    "Call_Vol = VALUES(Call_Vol), " +
                    "Total_Vol = VALUES(Total_Vol), " +
                    "Put_or_Call_Vol = VALUES(Put_or_Call_Vol), " +
                    "Put_OI = VALUES(Put_OI), " +
                    "Call_OI = VALUES(Call_OI), " +
                    "Total_OI = VALUES(Total_OI), " +
                    "Put_or_Call_OI = VALUES(Put_or_Call_OI), " +
                    "IV = VALUES(IV)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (JsonNode item : dataArray) {
                    String expirationDate = item.path("expirationDate").asText();
                    String expirationType = item.path("expirationType").asText("").trim().toLowerCase();

                    if ("n/a".equals(expirationType) || expirationType.isEmpty()) {
                        log("Skipping expirationType n/a for symbol: " + tickerFromDb);
                        continue;
                    }

                    char typeChar;
                    if ("weekly".equals(expirationType)) {
                        typeChar = 'w';
                    } else if ("monthly".equals(expirationType)) {
                        typeChar = 'm';
                    } else {
                        log("Skipping unknown expirationType '" + expirationType + "' for symbol: " + tickerFromDb);
                        continue;
                    }

                    LocalDate parsedDate = LocalDate.parse(expirationDate, inputFormatter);
                    String formattedDate = outputFormatter.format(parsedDate);
                    String expirationKey = formattedDate + "-" + typeChar;

                    Timestamp nyTimestamp = Timestamp.valueOf(ZonedDateTime.now(nyZone).toLocalDateTime());

                    // ---- Parse numeric fields ----
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

                    // ---- Set SQL parameters ----
                    stmt.setString(1, tickerFromDb);
                    stmt.setString(2, CycleHelper.getCycleRange());
                    stmt.setString(3, expirationKey);
                    stmt.setTimestamp(4, nyTimestamp);
                    stmt.setInt(5, daysToExpiration);
                    stmt.setInt(6, putVolume);
                    stmt.setInt(7, callVolume);
                    stmt.setInt(8, totalVolume);
                    stmt.setDouble(9, putCallVolumeRatio);
                    stmt.setInt(10, putOpenInterest);
                    stmt.setInt(11, callOpenInterest);
                    stmt.setInt(12, totalOpenInterest);
                    stmt.setDouble(13, putCallOpenInterestRatio);
                    stmt.setDouble(14, impliedVolatility);

                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }
    }


    private static String extractXsrfFromCookies(List<String> cookies) {
        return cookies.stream()
                .filter(c -> c.startsWith("XSRF-TOKEN="))
                .map(c -> c.substring("XSRF-TOKEN=".length(), c.indexOf(';')))
                .map(v -> URLDecoder.decode(v, StandardCharsets.UTF_8))
                .findFirst()
                .orElse("");
    }

    private static HttpClient createHttpClientForSymbol(InetSocketAddress proxy) {
        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);

        return HttpClient.newBuilder()
                .proxy(ProxySelector.of(proxy))
                .cookieHandler(cookieManager)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    private static void log(String message) {
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
        String line = "[" + timestamp + "] " + message;
        System.out.println(line);
        try {
            Files.writeString(logFile, line + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.err.println("Logging failed: " + e.getMessage());
        }
    }
}
