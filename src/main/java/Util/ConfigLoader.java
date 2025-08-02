package Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {

    private Properties props = new Properties();
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    public ConfigLoader() throws IOException {
        load();
    }

    private void load() throws IOException {
        String externalPath = System.getProperty("config.file"); // from -Dconfig.file
        InputStream input;
        if (externalPath != null) {
            System.out.println("Loading external config: " + externalPath);
            input = new FileInputStream(externalPath);
        } else {
            input = getClass().getClassLoader().getResourceAsStream("config.properties");
//            System.out.println("Loading default classpath config.properties");
        }

        if (input == null) {
            throw new IOException("Config file not found!");
        }
        props.load(input);
        input.close();

        this.dbUrl = props.getProperty("db.url");
        this.dbUser = props.getProperty("db.user");
        this.dbPassword = props.getProperty("db.password");
    }


    public String getDbUrl() {
        return dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
}
