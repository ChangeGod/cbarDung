package apicall;

import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import Util.ConfigLoader;
import Util.ProxyManager;
import Util.UserAgentProvider;

public class BarchartHtmlFetcher {

    public static void fetchAndStoreVolatilityData(String symbol, ConfigLoader config, ProxyManager proxyManager) {
        InetSocketAddress proxyUsed = null;
        try {
            // --- Pick proxy (same as processTicker) ---
            proxyUsed = proxyManager.getNextProxy();
            HttpClient client = HttpClient.newBuilder()
                    .proxy(ProxySelector.of(proxyUsed))
                    .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
                    .connectTimeout(Duration.ofSeconds(30))
                    .build();

            // --- Fetch page to get cookies ---
            String pageUrl = "https://www.barchart.com/stocks/quotes/" + symbol + "/put-call-ratios?orderBy=averageVolatility&orderDir=desc";
            HttpRequest pageRequest = HttpRequest.newBuilder()
                    .uri(URI.create(pageUrl))
                    .header("user-agent", UserAgentProvider.getRandomUserAgent())
                    .header("accept-language", "en-US,en;q=0.9")
                    .header("cache-control", "no-cache")
                    .header("accept", "text/html")
                    .GET()
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> pageResponse = client.send(pageRequest, HttpResponse.BodyHandlers.ofString());
            if (pageResponse.statusCode() != 200) {
                System.err.println("Failed to fetch page for " + symbol + " Status=" + pageResponse.statusCode());
                proxyManager.markProxyBad(proxyUsed);
                return;
            }

            // --- Build cookie header ---
            List<String> setCookies = pageResponse.headers().allValues("set-cookie");
            String cookieHeader = setCookies.stream()
                    .map(c -> c.split(";", 2)[0])
                    .collect(Collectors.joining("; "));

            // --- Parse volatility info from HTML ---
            Document doc = Jsoup.parse(pageResponse.body());
            Element hvElement = doc.select("span:contains(Historic Volatility:) ~ span strong").first();
            String historicVolatility = hvElement != null ? hvElement.text().replace("%", "") : "0";

            Element ivRankElement = doc.select("span:contains(IV Rank:) ~ span strong").first();
            String ivRank = ivRankElement != null ? ivRankElement.text().replace("%", "") : "0";

            Element ivPercentileElement = doc.select("span:contains(IV Percentile:) ~ span strong").first();
            String ivPercentile = ivPercentileElement != null ? ivPercentileElement.text().replace("%", "") : "0";

            double hvVal = parseDoubleSafe(historicVolatility);
            double ivRankVal = parseDoubleSafe(ivRank);
            double ivPercentileVal = parseDoubleSafe(ivPercentile);

            insertIntoDatabase(symbol, hvVal, ivRankVal, ivPercentileVal, config);

        } catch (Exception e) {
            System.err.println("Error fetching HTML for " + symbol + ": " + e.getMessage());
            if (proxyUsed != null) proxyManager.markProxyBad(proxyUsed);
        }
    }

    private static void insertIntoDatabase(String symbol, double hv, double ivRank, double ivPercentile, ConfigLoader config) {
        String sql = "UPDATE market_data " +
                "SET Historic_Volatility = ?, IV_Rank = ?, IV_Percentile = ? " +
                "WHERE symbol = ?";

        try (Connection conn = DriverManager.getConnection(config.getDbUrl(), config.getDbUser(), config.getDbPassword());
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setDouble(1, hv);
            stmt.setDouble(2, ivRank);
            stmt.setDouble(3, ivPercentile);
            stmt.setString(4, symbol);
            int rows = stmt.executeUpdate();
            if (rows == 0) {
                System.out.println("No existing row for symbol '" + symbol + "', consider inserting a new row.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double parseDoubleSafe(String value) {
        if (value == null) return 0.0;
        value = value.trim();
        if (value.isEmpty() || value.equalsIgnoreCase("N/A")) return 0.0;
        return Double.parseDouble(value);
    }
}
