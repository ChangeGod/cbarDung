package Util;

import java.net.*;
import java.net.http.HttpClient;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HttpHelper {

    public static HttpClient createHttpClientForSymbol(InetSocketAddress proxy) {
        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
        return HttpClient.newBuilder()
                .proxy(ProxySelector.of(proxy))
                .cookieHandler(cookieManager)
                .connectTimeout(java.time.Duration.ofSeconds(30))
                .build();
    }

    public static String extractXsrfFromCookies(List<String> cookies) {
        return cookies.stream()
                .filter(c -> c.startsWith("XSRF-TOKEN="))
                .map(c -> c.substring("XSRF-TOKEN=".length(), c.indexOf(';')))
                .map(v -> URLDecoder.decode(v, StandardCharsets.UTF_8))
                .findFirst().orElse("");
    }
}
