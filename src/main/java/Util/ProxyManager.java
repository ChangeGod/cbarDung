package Util;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ProxyManager {

    private final List<InetSocketAddress> proxies = new ArrayList<>();
    private final Set<InetSocketAddress> badProxies = Collections.synchronizedSet(new HashSet<>());
    private final AtomicInteger counter = new AtomicInteger(0);
    private final boolean useRotation;
    private final Consumer<String> logger;

    public ProxyManager(ConfigLoader config, Consumer<String> logger) {
        this.logger = logger;

        String proxyList = config.getProperty("proxy.list", "").trim();
        boolean rotationFlag = false;

        if (!proxyList.isEmpty()) {
            for (String proxy : proxyList.split(",")) {
                proxy = proxy.trim();
                if (proxy.isEmpty()) continue;

                // Handle range syntax: host:portStart-portEnd
                if (proxy.matches(".*:\\d+-\\d+")) {
                    try {
                        String host = proxy.substring(0, proxy.indexOf(":"));
                        String portRange = proxy.substring(proxy.indexOf(":") + 1);
                        String[] rangeParts = portRange.split("-");
                        int startPort = Integer.parseInt(rangeParts[0]);
                        int endPort = Integer.parseInt(rangeParts[1]);
                        for (int port = startPort; port <= endPort; port++) {
                            proxies.add(new InetSocketAddress(host, port));
                        }
                    } catch (Exception e) {
                        this.logger.accept("Invalid proxy range in config: " + proxy);
                    }
                } else {
                    // Normal host:port
                    String[] parts = proxy.split(":");
                    if (parts.length == 2) {
                        try {
                            proxies.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                        } catch (NumberFormatException e) {
                            this.logger.accept("Invalid proxy port in config: " + proxy);
                        }
                    }
                }
            }
            rotationFlag = !proxies.isEmpty();
        } else {
            String host = config.getProperty("proxy.host", "").trim();
            String port = config.getProperty("proxy.port", "").trim();
            if (!host.isEmpty() && !port.isEmpty()) {
                try {
                    proxies.add(new InetSocketAddress(host, Integer.parseInt(port)));
                } catch (NumberFormatException e) {
                    this.logger.accept("Invalid proxy port: " + port);
                }
            }
        }
        this.useRotation = rotationFlag;
    }


    private InetSocketAddress getNextProxy() {
        if (proxies.isEmpty()) return null;

        for (int i = 0; i < proxies.size(); i++) {
            int index = useRotation ? counter.getAndIncrement() % proxies.size() : 0;
            InetSocketAddress proxy = proxies.get(index);
            if (!badProxies.contains(proxy)) {
                return proxy;
            }
        }
        return null; // all proxies marked bad
    }

    public InetSocketAddress getCurrentProxy() {
        if (proxies.isEmpty()) return null;
        if (useRotation) {
            int index = (counter.get() - 1 + proxies.size()) % proxies.size();
            return proxies.get(index);
        }
        return proxies.get(0);
    }

    public void markProxyBad(InetSocketAddress proxy) {
        badProxies.add(proxy);
        logger.accept("Proxy marked as bad: " + proxy);
    }
}
