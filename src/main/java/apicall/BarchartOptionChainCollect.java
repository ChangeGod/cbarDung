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
import java.util.stream.Collectors;

import static Util.ProxyManager.getPublicIP;

public class BarchartOptionChainCollect {

    public static void collectOptionChains(String symbol, java.sql.Date updateDate,
                                           ConfigLoader config, ProxyManager proxyManager) throws Exception {

        List<ExpirationInfo> expirationList = getExpirationsForSymbol(symbol, config, updateDate);
        if (expirationList.isEmpty()) {
            LogUtil.log("No expiration dates found for " + symbol + " on " + updateDate);
            return;
        }

        for (ExpirationInfo expInfo : expirationList) {
            processOptionChain(symbol.toUpperCase().trim(), expInfo.date.trim(), expInfo.type.trim(), config, proxyManager);
        }
//        LogUtil.log("Option chain data fetched and saved for all expirations of " + symbol);
    }

    public static void main(String[] args) throws Exception {
        ConfigLoader config = new ConfigLoader();
        ProxyManager proxyManager = new ProxyManager(config, LogUtil::log);

        if (args.length == 1) {
            String symbol = args[0];
            java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
            collectOptionChains(symbol, today, config, proxyManager);

        } else if (args.length >= 3) {
            String symbol = args[0];
            String expirationDate = args[1];
            String expirationType = args[2];
            initLogFile(config);
            LogUtil.log("Processing expiration " + expirationDate + " (" + expirationType + ") for " + symbol);
            processOptionChain(symbol.toUpperCase().trim(), expirationDate.trim(), expirationType.trim(), config, proxyManager);
        } else {
            System.out.println("Usage:");
            System.out.println("  java -cp yourJar.jar apicall.BarchartOptionChainCollect <SYMBOL>");
            System.out.println("    -> fetch all expirations for today from DB");
            System.out.println("  java -cp yourJar.jar apicall.BarchartOptionChainCollect <SYMBOL> <EXPIRATION> <TYPE>");
            System.out.println("    -> fetch specific expiration and type");
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

    private static void processOptionChain(String symbol, String expirationDate, String expirationType,
                                           ConfigLoader config, ProxyManager proxyManager) {
        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            InetSocketAddress proxyUsed = null;
            try {
                proxyUsed = proxyManager.getNextProxy();
                HttpClient client = HttpHelper.createHttpClientForSymbol(proxyUsed);

                String pageUrl = "https://www.barchart.com/stocks/quotes/" + symbol + "/options?expiration=" + expirationDate + "-" + expirationType.charAt(0);
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

                // Single Combined API Call
                String combinedUrl = "https://www.barchart.com/proxies/core-api/v1/options/get"
                        + "?baseSymbol=" + symbol
                        + "&fields=symbol,baseSymbol,strikePrice,expirationDate,moneyness,"
                        + "bidPrice,midpoint,askPrice,lastPrice,priceChange,percentChange,"
                        + "volume,openInterest,openInterestChange,volatility,delta,gamma,theta,vega,rho,"
                        + "volumeOpenInterestRatio,itmProbability,optionType,daysToExpiration,"
                        + "tradeTime,averageVolatility,historicVolatility30d,baseNextEarningsDate,"
                        + "dividendExDate,baseTimeCode,expirationType,impliedVolatilityRank1y,"
                        + "symbolCode,symbolType,theoretical"
                        + "&groupBy=optionType&expirationDate=" + expirationDate
                        + "&meta=field.shortName,expirations,field.description&orderBy=strikePrice&orderDir=asc"
                        + "&optionsOverview=true&expirationType=" + expirationType + "&raw=1";

                HttpRequest combinedRequest = HttpRequest.newBuilder()
                        .uri(URI.create(combinedUrl))
                        .GET()
                        .timeout(java.time.Duration.ofSeconds(30))
                        .header("accept", "application/json")
                        .header("cookie", cookieHeader)
                        .header("x-xsrf-token", xsrfToken)
                        .header("referer", pageUrl)
                        .build();

                HttpResponse<String> combinedResponse = client.send(combinedRequest, HttpResponse.BodyHandlers.ofString());

                if (combinedResponse.statusCode() == 200) {
                    saveOptionChain(combinedResponse.body(), symbol, expirationDate, expirationType);
                    break;
                } else {
                    LogUtil.log("Error response for " + symbol + ": status=" + combinedResponse.statusCode());
                }
            } catch (Exception e) {
                LogUtil.log("Error fetching option chain " + symbol + " (attempt " + attempt + "): " + e.getMessage());
                if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
            }
            try { Thread.sleep(2000 * attempt); } catch (InterruptedException ignored) {}
        }
    }

    private static void saveOptionChain(String json, String baseSymbol,
                                        String expirationDate, String expirationType) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json).path("data");

        ZoneId nyZone = ZoneId.of("America/New_York");
        ZonedDateTime nyNow = ZonedDateTime.now(nyZone);
        java.sql.Date updateDate = java.sql.Date.valueOf(nyNow.toLocalDate());
        java.sql.Time updateTime = java.sql.Time.valueOf(nyNow.toLocalTime().withNano(0));

        int batchSize = 200;
        int count = 0; // Row counter for batch size
        try (Connection conn = ConnectionPool.getConnection()) {
            String sql = "INSERT INTO option_chain_data(" +
                    "symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time, " +
                    "contract_type, strike, moneyness, bid, mid, ask, last, theoretical, " +
                    "change_val, pct_chg, volume, open_interest, oi_change, iv, delta, gamma, " +
                    "theta, vega, rho, vol_oi_ratio, itm_probability, time_quoted) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                    "ON DUPLICATE KEY UPDATE " +
                    "moneyness=VALUES(moneyness), bid=VALUES(bid), mid=VALUES(mid), ask=VALUES(ask), " +
                    "last=VALUES(last), theoretical=VALUES(theoretical), change_val=VALUES(change_val), " +
                    "pct_chg=VALUES(pct_chg), volume=VALUES(volume), open_interest=VALUES(open_interest), " +
                    "oi_change=VALUES(oi_change), iv=VALUES(iv), delta=VALUES(delta), gamma=VALUES(gamma), " +
                    "theta=VALUES(theta), vega=VALUES(vega), rho=VALUES(rho), vol_oi_ratio=VALUES(vol_oi_ratio), " +
                    "itm_probability=VALUES(itm_probability), time_quoted=VALUES(time_quoted)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (String type : List.of("Call", "Put")) {
                    if (root.has(type)) {
                        for (JsonNode node : root.get(type)) {
                            String contractType = type.equals("Call") ? "C" : "P";
                            addOptionRecord(stmt, node, baseSymbol, expirationDate, expirationType, updateDate, updateTime, contractType);
                        }
                    }
                }

                stmt.addBatch(); // Add the option record to the batch

                count++;
                if (count % batchSize == 0) {
                    stmt.executeBatch(); // Execute the batch
                    count = 0; // Reset counter after batch commit
                }

                if (count > 0) {
                    stmt.executeBatch(); // Execute any remaining rows in the batch
                }
            }
        }
    }

    private static void addOptionRecord(PreparedStatement stmt, JsonNode node,
                                        String baseSymbol, String expirationDate, String expirationType,
                                        java.sql.Date updateDate, java.sql.Time updateTime,
                                        String contractType) throws SQLException {
        stmt.setString(1, baseSymbol);
        stmt.setString(2, CycleHelper.getCycleRange());
        stmt.setString(3, expirationDate);
        stmt.setString(4, expirationType);
        stmt.setDate(5, updateDate);
        stmt.setTime(6, updateTime);
        stmt.setString(7, contractType);

        stmt.setDouble(8,  NumberParser.parseDoubleSafe(node.path("strikePrice").asText()));
        stmt.setString(9,  node.path("moneyness").asText());
        stmt.setDouble(10, NumberParser.parseDoubleSafe(node.path("bidPrice").asText()));
        stmt.setDouble(11, NumberParser.parseDoubleSafe(node.path("midpoint").asText()));
        stmt.setDouble(12, NumberParser.parseDoubleSafe(node.path("askPrice").asText()));
        stmt.setDouble(13, NumberParser.parseDoubleSafe(node.path("lastPrice").asText()));
        stmt.setDouble(14, NumberParser.parseDoubleSafe(node.path("theoretical").asText()));
        stmt.setDouble(15, NumberParser.parseDoubleSafe(node.path("priceChange").asText()));
        stmt.setString(16, node.path("percentChange").asText());
        stmt.setInt(17, NumberParser.parseIntSafe(node.path("volume").asText()));
        stmt.setInt(18, NumberParser.parseIntSafe(node.path("openInterest").asText()));
        stmt.setString(19, node.path("openInterestChange").asText());
        stmt.setDouble(20, NumberParser.parseDoubleSafe(node.path("volatility").asText().replace("%", "")));
        stmt.setDouble(21, NumberParser.parseDoubleSafe(node.path("delta").asText()));
        stmt.setDouble(22, NumberParser.parseDoubleSafe(node.path("gamma").asText()));
        stmt.setDouble(23, NumberParser.parseDoubleSafe(node.path("theta").asText()));
        stmt.setDouble(24, NumberParser.parseDoubleSafe(node.path("vega").asText()));
        stmt.setDouble(25, NumberParser.parseDoubleSafe(node.path("rho").asText()));
        stmt.setDouble(26, NumberParser.parseDoubleSafe(node.path("volumeOpenInterestRatio").asText()));

        double itmProbability = 0.0;
        if (node.has("raw") && node.path("raw").has("itmProbability")) {
            itmProbability = node.path("raw").path("itmProbability").asDouble();
        }
        stmt.setDouble(27, itmProbability);
        stmt.setString(28, node.path("tradeTime").asText());

        stmt.addBatch();
    }

    private static List<ExpirationInfo> getExpirationsForSymbol(String symbol, ConfigLoader config, java.sql.Date updateDate) throws SQLException {
        List<ExpirationInfo> list = new ArrayList<>();
        String sql = "SELECT DISTINCT expiration_date, expiration_type FROM market_data WHERE symbol = ? AND update_date = ?";
        try ( Connection conn = ConnectionPool.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, symbol);
            stmt.setDate(2, updateDate);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    list.add(new ExpirationInfo(rs.getString(1), rs.getString(2)));
                }
            }
        }
        return list;
    }

    private static class ExpirationInfo {
        String date;
        String type;
        ExpirationInfo(String date, String type) {
            this.date = date;
            this.type = type;
        }
    }
}
