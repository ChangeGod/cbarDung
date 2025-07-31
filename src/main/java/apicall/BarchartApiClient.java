package apicall;

import Util.ConfigLoader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class BarchartApiClient {

    private static String LOG_FILE;

    public static void main(String[] args) throws Exception {
        // Load configuration
        ConfigLoader config = new ConfigLoader();
        LOG_FILE = config.getProperty("log.path", "log.txt");

        var client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // STEP 1: Visit the page to bootstrap cookies
        String pageUrl = "https://www.barchart.com/stocks/quotes/UNH/volatility-greeks?expiration=2025-10-17-m";

        var pageRequest = HttpRequest.newBuilder()
                .uri(URI.create(pageUrl))
                .GET()
                .timeout(Duration.ofSeconds(10))
                .header("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                .header("accept-language", "en-US,en;q=0.9,vi;q=0.8")
                .header("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .build();

        var pageResponse = client.send(pageRequest, HttpResponse.BodyHandlers.ofString());

        List<String> setCookies = pageResponse.headers().allValues("set-cookie");
        String cookieHeader = setCookies.stream()
                .map(c -> c.split(";", 2)[0])
                .filter(c -> !c.startsWith("lastPage="))
                .collect(Collectors.joining("; "));

        if (!cookieHeader.contains("bcFreeUserPageView")) {
            cookieHeader += "; bcFreeUserPageView=0";
        }

        String xsrfToken = extractXsrfFromCookies(setCookies);

        log("Cookies Sent: " + cookieHeader);
        log("XSRF Token: " + xsrfToken);

        String apiUrl = "https://www.barchart.com/proxies/core-api/v1/options/get"
                + "?fields=symbol%2CbaseSymbol%2CstrikePrice%2CexpirationDate%2ClastPrice%2Ctheoretical%2Cvolatility%2Cdelta"
                + "%2Cgamma%2Ctheta%2Cvega%2Crho%2Cvolume%2CopenInterest%2CvolumeOpenInterestRatio%2CitmProbability"
                + "%2CoptionType%2CdaysToExpiration%2CexpirationType%2CtradeTime%2ChistoricVolatility30d%2CbaseNextEarningsDate"
                + "%2CdividendExDate%2CbaseTimeCode%2CimpliedVolatilityRank1y%2CaverageVolatility%2CsymbolCode%2CsymbolType"
                + "&baseSymbol=UNH"
                + "&groupBy=optionType"
                + "&expirationDate=2025-10-17"
                + "&meta=field.shortName%2Cexpirations%2Cfield.description"
                + "&orderBy=strikePrice"
                + "&orderDir=asc"
                + "&expirationType=monthly"
                + "&raw=1";

        var apiRequest = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .GET()
                .timeout(Duration.ofSeconds(10))
                .header("accept", "application/json")
                .header("accept-language", "en-US,en;q=0.9,vi;q=0.8")
                .header("cookie", cookieHeader)
                .header("priority", "u=1, i")
                .header("referer", pageUrl)
                .header("sec-ch-ua", "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Microsoft Edge\";v=\"138\"")
                .header("sec-ch-ua-mobile", "?0")
                .header("sec-ch-ua-platform", "\"Windows\"")
                .header("sec-fetch-dest", "empty")
                .header("sec-fetch-mode", "cors")
                .header("sec-fetch-site", "same-origin")
                .header("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0")
                .header("x-xsrf-token", xsrfToken)
                .build();

        var apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());

        log("API Status: " + apiResponse.statusCode());

        if (apiResponse.statusCode() == 200) {
            saveResponseToFile(apiResponse.body(), "output.json");
            log("Response saved to output.json");
        } else {
            log("Failed to fetch data: " + apiResponse.body());
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

    private static void saveResponseToFile(String response, String filename) throws IOException {
        Path filePath = Path.of(filename);
        Files.writeString(filePath, response, StandardCharsets.UTF_8);
    }

    private static void log(String message) throws IOException {
        System.out.println(message);
        Files.writeString(
                Path.of(LOG_FILE),
                message + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }
}
