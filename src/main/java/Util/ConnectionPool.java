package Util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
    private static HikariDataSource dataSource;

    public static void init(ConfigLoader config, int threadCount) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setJdbcUrl(config.getDbUrl());
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());

        // ---- Dynamically determine pool size ----
        int maxPool = Math.max(threadCount, Runtime.getRuntime().availableProcessors() * 2);
        int minIdle = Math.max(5, maxPool / 4);

        hikariConfig.setMaximumPoolSize(maxPool);
        hikariConfig.setMinimumIdle(minIdle);

        hikariConfig.setIdleTimeout(300_000);       // 5 minutes
        hikariConfig.setConnectionTimeout(5_000);   // 5 seconds
        hikariConfig.setMaxLifetime(1_800_000);     // 30 minutes

        LogUtil.log("🔌 HikariCP pool: maxPoolSize=" + maxPool + ", minIdle=" + minIdle);

        dataSource = new HikariDataSource(hikariConfig);
    }


    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
