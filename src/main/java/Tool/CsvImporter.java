package Tool;

import Util.ConfigLoader;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CsvImporter {

    public static void main(String[] args) {
        String csvFile = "C:\\Project\\Paul\\Paul\\src\\main\\resources\\Option Data - List.csv"; // your CSV file path

        try {
            // Load DB config
            ConfigLoader config = new ConfigLoader();

            // DB Connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(
                    config.getDbUrl(), config.getDbUser(), config.getDbPassword())) {

                String sql = "INSERT INTO symbol_list (ticker, company, sector, industry, country) "
                        + "VALUES (?, ?, ?, ?, ?)";

                try (CSVReader reader = new CSVReader(new FileReader(csvFile));
                     PreparedStatement ps = conn.prepareStatement(sql)) {

                    String[] line;
                    boolean isFirstLine = true;

                    while ((line = reader.readNext()) != null) {
                        if (isFirstLine) { // skip header row
                            isFirstLine = false;
                            continue;
                        }

                        // CSV columns: No. | Ticker | Company | Sector | Industry | Country | ...
                        String ticker = line[1];
                        String company = line[2];
                        String sector = line[3];
                        String industry = line[4];
                        String country = line[5];

                        ps.setString(1, ticker);
                        ps.setString(2, company);
                        ps.setString(3, sector);
                        ps.setString(4, industry);
                        ps.setString(5, country);

                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
            System.out.println("CSV import completed!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
