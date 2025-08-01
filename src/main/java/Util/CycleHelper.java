package Util;

import java.time.LocalTime;
import java.time.ZoneId;

public class CycleHelper {

    public static String getCycleRange() {
        LocalTime now = LocalTime.now(ZoneId.of("America/New_York"));

        if (!now.isBefore(LocalTime.of(9, 36)) && !now.isAfter(LocalTime.of(10, 5))) return "Khung 1: 9:36 – 10:05";
        if (!now.isBefore(LocalTime.of(10, 6)) && !now.isAfter(LocalTime.of(10, 35))) return "Khung 2: 10:06 – 10:35";
        if (!now.isBefore(LocalTime.of(10, 36)) && !now.isAfter(LocalTime.of(11, 5))) return "Khung 3: 10:36 – 11:05";
        if (!now.isBefore(LocalTime.of(11, 6)) && !now.isAfter(LocalTime.of(11, 35))) return "Khung 4: 11:06 – 11:35";
        if (!now.isBefore(LocalTime.of(11, 36)) && !now.isAfter(LocalTime.of(12, 5))) return "Khung 5: 11:36 – 12:05";
        if (!now.isBefore(LocalTime.of(12, 6)) && !now.isAfter(LocalTime.of(12, 35))) return "Khung 6: 12:06 – 12:35";
        if (!now.isBefore(LocalTime.of(12, 36)) && !now.isAfter(LocalTime.of(13, 5))) return "Khung 7: 12:36 – 1:05";
        if (!now.isBefore(LocalTime.of(13, 6)) && !now.isAfter(LocalTime.of(13, 35))) return "Khung 8: 1:06 – 1:35";
        if (!now.isBefore(LocalTime.of(13, 36)) && !now.isAfter(LocalTime.of(14, 5))) return "Khung 9: 1:36 – 2:05";
        if (!now.isBefore(LocalTime.of(14, 6)) && !now.isAfter(LocalTime.of(14, 35))) return "Khung 10: 2:06 – 2:35";
        if (!now.isBefore(LocalTime.of(14, 36)) && !now.isAfter(LocalTime.of(15, 5))) return "Khung 11: 2:36 – 3:05";
        if (!now.isBefore(LocalTime.of(15, 6)) && !now.isAfter(LocalTime.of(15, 35))) return "Khung 12: 3:06 – 3:35";
        if (!now.isBefore(LocalTime.of(15, 36)) && !now.isAfter(LocalTime.of(16, 5))) return "Khung 13: 3:36 – 4:05";
        if (!now.isBefore(LocalTime.of(16, 6)) && !now.isAfter(LocalTime.of(16, 35))) return "Khung 14: 4:06 – 4:35";

        return "Outside cycle";
    }
}
