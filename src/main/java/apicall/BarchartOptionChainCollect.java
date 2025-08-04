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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static Util.ProxyManager.getPublicIP;

public class BarchartOptionChainCollect {

    // --- Public method for BarchartCollect to call directly ---
    public static void collectOptionChains(String symbol, java.sql.Date updateDate,
                                           ConfigLoader config, ProxyManager proxyManager) throws Exception {
        initLogFile(config);

        List<String> expirationDates = getExpirationsForSymbol(symbol, config, updateDate);
        if (expirationDates.isEmpty()) {
            LogUtil.log("No expiration dates found for " + symbol + " on " + updateDate);
            return;
        }

        for (String expirationDate : expirationDates) {
            LogUtil.log("Processing expiration " + expirationDate + " for " + symbol);
            processOptionChain(symbol.toUpperCase().trim(), expirationDate.trim(), config, proxyManager);
        }
    }


    // --- CLI Main Method ---
    public static void main(String[] args) throws Exception {
        ConfigLoader config = new ConfigLoader();
        ProxyManager proxyManager = new ProxyManager(config, LogUtil::log);

        if (args.length == 1) {
            // Auto mode: use today's update_date expirations from DB
            String symbol = args[0];
            java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
            collectOptionChains(symbol, today, config, proxyManager);

        } else if (args.length >= 2) {
            // Manual mode: explicit expiration list
            String symbol = args[0];
            List<String> expirations = java.util.Arrays.asList(args).subList(1, args.length);

            // Keep old logic but call private processOptionChain directly
            initLogFile(config);
            for (String expirationDate : expirations) {
                LogUtil.log("Processing expiration " + expirationDate + " for " + symbol);
                processOptionChain(symbol.toUpperCase().trim(), expirationDate.trim(), config, proxyManager);
            }
        } else {
            System.out.println("Usage:");
            System.out.println("  java -cp yourJar.jar apicall.BarchartOptionChainCollect <SYMBOL>");
            System.out.println("    -> fetch all expirations for today from DB");
            System.out.println("  java -cp yourJar.jar apicall.BarchartOptionChainCollect <SYMBOL> <EXPIRATION1> [<EXPIRATION2> ...]");
            System.out.println("    -> fetch only specified expiration dates");
        }
    }


    private static void initLogFile(ConfigLoader config) throws Exception {
        String logDir = config.getProperty("log.path", ".");
        Path logDirPath = Paths.get(logDir);
        if (!Files.exists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").format(LocalDateTime.now());
        Path logFile = logDirPath.resolve("barchart_option_chain_" + timestamp + ".log");
        LogUtil.init(logFile);
    }

    private static void processOptionChain(String symbol, String expirationDate,
                                           ConfigLoader config, ProxyManager proxyManager) {
        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                proxyUsed = proxyManager.getNextProxy();
                HttpClient client = HttpHelper.createHttpClientForSymbol(proxyUsed);

                LogUtil.log("Fetching options chain for " + symbol + " expiration " + expirationDate
                        + " using proxy: " + proxyUsed + ", public IP: " + getPublicIP(client));

                String pageUrl = "https://www.barchart.com/stocks/quotes/" + symbol + "/options?expiration=" + expirationDate + "-w";
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

                String apiUrl = "https://www.barchart.com/proxies/core-api/v1/options/get"
                        + "?baseSymbol=" + symbol
                        + "&fields=symbol,baseSymbol,strikePrice,expirationDate,moneyness,bidPrice,midpoint,"
                        + "askPrice,lastPrice,priceChange,percentChange,volume,openInterest,openInterestChange,"
                        + "volatility,delta,optionType,daysToExpiration,tradeTime,averageVolatility,historicVolatility30d,"
                        + "baseNextEarningsDate,dividendExDate,baseTimeCode,expirationType,impliedVolatilityRank1y,"
                        + "symbolCode,symbolType"
                        + "&groupBy=optionType&expirationDate=" + expirationDate
                        + "&meta=field.shortName,expirations,field.description&orderBy=strikePrice&orderDir=asc"
                        + "&optionsOverview=true&expirationType=weekly&raw=1";

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
                    saveOptionChain(apiResponse.body(), config, symbol, expirationDate);
                    LogUtil.log("Option chain saved for " + symbol + " " + expirationDate);
                    break;
                } else {
                    LogUtil.log("Error " + apiResponse.statusCode() + " for " + symbol + " " + expirationDate);
                }
            } catch (Exception e) {
                LogUtil.log("Error fetching option chain " + symbol + " (attempt " + attempt + "): " + e.getMessage());
                if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
            }
            try { Thread.sleep(2000 * attempt); } catch (InterruptedException ignored) {}
        }
    }

    private static void saveOptionChain(String jsonResponse, ConfigLoader config,
                                        String baseSymbol, String expirationDate) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);
        JsonNode dataNode = root.path("data");

        ZoneId nyZone = ZoneId.of("America/New_York");
        ZonedDateTime nyNow = ZonedDateTime.now(nyZone);
        java.sql.Date updateDate = java.sql.Date.valueOf(nyNow.toLocalDate());
        java.sql.Time updateTime = java.sql.Time.valueOf(nyNow.toLocalTime().withNano(0));

        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword())) {
            String sql = "INSERT INTO option_chain_data(" +
                    "symbol, Cycle_Range, expiration_date, update_date, update_time, " +
                    "contract_type, strike, moneyness, bid, mid, ask, last, " +
                    "change_val, pct_chg, volume, open_interest, oi_change, iv, delta, time_quoted, links) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "moneyness=VALUES(moneyness), bid=VALUES(bid), mid=VALUES(mid), ask=VALUES(ask), " +
                    "last=VALUES(last), change_val=VALUES(change_val), pct_chg=VALUES(pct_chg), " +
                    "volume=VALUES(volume), open_interest=VALUES(open_interest), oi_change=VALUES(oi_change), " +
                    "iv=VALUES(iv), delta=VALUES(delta), time_quoted=VALUES(time_quoted), links=VALUES(links)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                if (dataNode.has("Call")) {
                    for (JsonNode call : dataNode.get("Call")) {
                        addOptionRecord(stmt, call, baseSymbol, expirationDate, updateDate, updateTime, "C");
                    }
                }
                if (dataNode.has("Put")) {
                    for (JsonNode put : dataNode.get("Put")) {
                        addOptionRecord(stmt, put, baseSymbol, expirationDate, updateDate, updateTime, "P");
                    }
                }
                stmt.executeBatch();
            }
        }
    }

    private static void addOptionRecord(PreparedStatement stmt, JsonNode node,
                                        String baseSymbol, String expirationDate,
                                        java.sql.Date updateDate, java.sql.Time updateTime,
                                        String contractType) throws SQLException {
        stmt.setString(1, baseSymbol);
        stmt.setString(2, CycleHelper.getCycleRange());
        stmt.setString(3, expirationDate);
        stmt.setDate(4, updateDate);
        stmt.setTime(5, updateTime);
        stmt.setString(6, contractType);
        stmt.setDouble(7, NumberParser.parseDoubleSafe(node.path("strikePrice").asText()));
        stmt.setString(8, node.path("moneyness").asText());
        stmt.setDouble(9, NumberParser.parseDoubleSafe(node.path("bidPrice").asText()));
        stmt.setDouble(10, NumberParser.parseDoubleSafe(node.path("midpoint").asText()));
        stmt.setDouble(11, NumberParser.parseDoubleSafe(node.path("askPrice").asText()));
        stmt.setDouble(12, NumberParser.parseDoubleSafe(node.path("lastPrice").asText()));
        stmt.setDouble(13, NumberParser.parseDoubleSafe(node.path("priceChange").asText()));
        stmt.setString(14, node.path("percentChange").asText());
        stmt.setInt(15, NumberParser.parseIntSafe(node.path("volume").asText()));
        stmt.setInt(16, NumberParser.parseIntSafe(node.path("openInterest").asText()));
        stmt.setString(17, node.path("openInterestChange").asText());
        stmt.setDouble(18, NumberParser.parseDoubleSafe(node.path("volatility").asText().replace("%", "")));
        stmt.setDouble(19, NumberParser.parseDoubleSafe(node.path("delta").asText()));
        stmt.setString(20, node.path("tradeTime").asText());
        stmt.setString(21, ""); // links placeholder
        stmt.addBatch();
    }

    private static List<String> getExpirationsForSymbol(String symbol, ConfigLoader config, java.sql.Date updateDate) throws SQLException {
        List<String> expirations = new ArrayList<>();
        String sql = "SELECT DISTINCT expiration_date FROM market_data WHERE symbol = ? AND update_date = ?";
        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword());
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, symbol);
            stmt.setDate(2, updateDate);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    expirations.add(rs.getString(1));
                }
            }
        }
        return expirations;
    }

}
