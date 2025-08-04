package Util;

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogUtil {

    private static Path logFile;

    public static void init(Path logFilePath) {
        logFile = logFilePath;
    }

    public static void log(String message) {
        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
        String line = "[" + timestamp + "] " + message;
        System.out.println(line);
        if (logFile != null) {
            try {
                Files.writeString(logFile, line + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (Exception e) {
                System.err.println("Logging failed: " + e.getMessage());
            }
        }
    }
}
