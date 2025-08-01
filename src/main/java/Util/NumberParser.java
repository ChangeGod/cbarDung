package Util;

public class NumberParser {

    public static int parseIntSafe(String text) {
        if (text == null || text.isEmpty()) return 0;
        try {
            return Integer.parseInt(text.replace(",", "").trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static double parseDoubleSafe(String text) {
        if (text == null || text.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(text.replace(",", "").trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public static double parseVolatility(String text) {
        if (text == null || text.isEmpty()) return 0.0;
        text = text.replace("%", "").trim();
        return parseDoubleSafe(text) / 100.0;
    }
}
