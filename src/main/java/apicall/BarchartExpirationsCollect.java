package apicall;

import Util.ConfigLoader;
import Util.ProxyManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
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

public class BarchartExpirationsCollect {

    private static Path logFile;
    private static final Object rateLimitLock = new Object();
    private static volatile long lastRateLimitWait = 0;

    public static void main(String[] args) throws Exception {
        ConfigLoader config = new ConfigLoader();

        // ---- Prepare log file path ----
        String logDir = config.getProperty("log.path", ".");
        Path logDirPath = Paths.get(logDir);
        if (!Files.exists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").format(LocalDateTime.now());
        logFile = logDirPath.resolve("barchart_" + timestamp + ".log");

        // ---- Initialize ProxyManager with logger ----
        ProxyManager proxyManager = new ProxyManager(config, message -> log(message));

        List<String> tickers = loadTickers(config);
        if (tickers.isEmpty()) {
            log("No tickers found in symbol_list.");
            return;
        }

        int threadCount = Runtime.getRuntime().availableProcessors();
        log("Thread count set to: " + threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (String ticker : tickers) {
            executor.submit(() -> processTicker(ticker, config, proxyManager));
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);

        log("All tickers processed.");
    }

    private static void handleRateLimit() {
        synchronized (rateLimitLock) {
            long now = System.currentTimeMillis();
            if (now - lastRateLimitWait > 30000) {
                log("Global rate limit hit, waiting 30s before continuing...");
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                lastRateLimitWait = System.currentTimeMillis();
            } else {
                log("Rate limit recently handled, skipping additional wait.");
            }
        }
    }

    private static void processTicker(String ticker, ConfigLoader config, ProxyManager proxyManager) {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                HttpClient client = proxyManager.getHttpClient();
                proxyUsed = proxyManager.getCurrentProxy();

                // Log which proxy is being used for this ticker
                log("Processing " + ticker + " using proxy: " + proxyUsed);

                String pageUrl = "https://www.barchart.com/stocks/quotes/" + ticker + "/put-call-ratios";
                HttpRequest pageRequest = HttpRequest.newBuilder()
                        .uri(URI.create(pageUrl))
                        .GET()
                        .timeout(Duration.ofSeconds(30))
                        .header("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .header("accept", "text/html")
                        .build();
                HttpResponse<String> pageResponse = client.send(pageRequest, HttpResponse.BodyHandlers.ofString());

                List<String> setCookies = pageResponse.headers().allValues("set-cookie");
                String cookieHeader = setCookies.stream()
                        .map(c -> c.split(";", 2)[0])
                        .collect(Collectors.joining("; ")) + "; bcFreeUserPageView=0";
                String xsrfToken = extractXsrfFromCookies(setCookies);

                String apiUrl = "https://www.barchart.com/proxies/core-api/v1/options-expirations/get"
                        + "?fields=expirationDate%2CexpirationType%2CsymbolCode"
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
                        .header("sec-fetch-dest", "empty")
                        .header("sec-fetch-mode", "cors")
                        .header("sec-fetch-site", "same-origin")
                        .header("sec-ch-ua", "\"Chromium\";v=\"120\", \"Not-A.Brand\";v=\"99\"")
                        .header("sec-ch-ua-mobile", "?0")
                        .header("sec-ch-ua-platform", "\"Windows\"")
                        .build();

                HttpResponse<String> apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());

                if (apiResponse.statusCode() == 200) {
                    saveToDatabase(apiResponse.body(), config, ticker);
                    log("Data inserted/updated for: " + ticker);
                    break; // success, stop retry loop
                } else if (apiResponse.statusCode() == 429) {
                    log("Rate limit hit for " + ticker);
                    handleRateLimit();
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

    private static List<String> loadTickers(ConfigLoader config) throws Exception {
        List<String> tickers = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(
                config.getDbUrl(), config.getDbUser(), config.getDbPassword());
             PreparedStatement stmt = conn.prepareStatement("SELECT ticker FROM symbol_list");
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

            String sql = "INSERT INTO market_data(symbol, expiration_date, update_time) "
                    + "VALUES(?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE update_time = VALUES(update_time)";

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

                    stmt.setString(1, tickerFromDb);
                    stmt.setString(2, expirationKey);
                    stmt.setTimestamp(3, nyTimestamp);
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
