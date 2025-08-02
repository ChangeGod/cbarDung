package apicall;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import Util.ConfigLoader;

public class BarchartHtmlFetcher {

    /**
     * Fetch HTML from Barchart, parse volatility data, and update database.
     *
     * @param symbol Stock ticker (e.g., "AAPL")
     */
    public static void fetchAndStoreVolatilityData(String symbol) {
        try {
            // --- Load DB config ---
            ConfigLoader config = new ConfigLoader();

            // --- Fetch HTML ---
            String url = "https://www.barchart.com/stocks/quotes/" + symbol + "/put-call-ratios?orderBy=averageVolatility&orderDir=desc";
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("user-agent",
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0")
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String html = response.body();

                // --- Parse HTML ---
                Document doc = Jsoup.parse(html);

                Element hvElement = doc.select("span:contains(Historic Volatility:) ~ span strong").first();
                String historicVolatility = hvElement != null ? hvElement.text().replace("%", "") : "0";

                Element ivRankElement = doc.select("span:contains(IV Rank:) ~ span strong").first();
                String ivRank = ivRankElement != null ? ivRankElement.text().replace("%", "") : "0";

                Element ivPercentileElement = doc.select("span:contains(IV Percentile:) ~ span strong").first();
                String ivPercentile = ivPercentileElement != null ? ivPercentileElement.text().replace("%", "") : "0";

//                System.out.println("Historic Volatility: " + historicVolatility + "%");
//                System.out.println("IV Rank: " + ivRank + "%");
//                System.out.println("IV Percentile: " + ivPercentile + "%");

                // --- Insert into DB ---
                double hvVal = parseDoubleSafe(historicVolatility);
                double ivRankVal = parseDoubleSafe(ivRank);
                double ivPercentileVal = parseDoubleSafe(ivPercentile);

                insertIntoDatabase(symbol, hvVal, ivRankVal, ivPercentileVal, config);



            } else {
                System.err.println("Failed to fetch HTML. Status: " + response.statusCode());
            }

        } catch (Exception e) {
            e.printStackTrace();
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
            } else {
//                System.out.println("Database updated successfully for " + symbol);
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



    // --- Simple entry point ---
    public static void main(String[] args) {
        fetchAndStoreVolatilityData("AAPL");
    }
}
