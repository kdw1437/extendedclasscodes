package com.jurosys.extension.com;

import com.ibm.icu.util.ChineseCalendar;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LunarCalendar {
    public static final int LD_SUNDAY = 7;
    public static final int LD_SATURDAY = 6;
    public static final int LD_MONDAY = 1;
    static Map<Integer, Set<LocalDate>> map = new HashMap<>();

    private LocalDate lunarToSolar(LocalDate lunar) {
        ChineseCalendar cc = new ChineseCalendar();
        cc.set(ChineseCalendar.EXTENDED_YEAR, lunar.getYear() + 2637);   // year + 2637
        cc.set(ChineseCalendar.MONTH, lunar.getMonthValue() - 1);        // month -1
        cc.set(ChineseCalendar.DAY_OF_MONTH, lunar.getDayOfMonth());     // day

        return Instant.ofEpochMilli(cc.getTimeInMillis()).atZone(ZoneId.of("UTC")).toLocalDate();
    }

    public Set<LocalDate> holidaySet(int year) {
        if (map.containsKey(year)) return map.get(year);
        Set<LocalDate> holidaysSet = new HashSet<>();

        // Solar holidays
        holidaysSet.add(LocalDate.of(year, 1, 1));   // New Year's Day
        holidaysSet.add(LocalDate.of(year, 3, 1));   // Independence Movement Day
        holidaysSet.add(LocalDate.of(year, 5, 5));   // Children's Day
        holidaysSet.add(LocalDate.of(year, 6, 6));   // Memorial Day
        holidaysSet.add(LocalDate.of(year, 8, 15));  // Liberation Day
        holidaysSet.add(LocalDate.of(year, 10, 3));  // National Foundation Day
        holidaysSet.add(LocalDate.of(year, 10, 9));  // Hangul Proclamation Day
        holidaysSet.add(LocalDate.of(year, 12, 25)); // Christmas Day

        // Lunar holidays
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 1)).minusDays(1));  // Lunar New Year Eve
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 1)));               // Lunar New Year
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 2)));               // Lunar New Year Day 2
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 4, 8)));               // Buddha's Birthday
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 14)));              // Chuseok Eve
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 15)));              // Chuseok
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 16)));              // Chuseok Day 2

        try {
            // Substitute holidays
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 5, 5)));  // Children's Day
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 3, 1)));  // Independence Movement Day
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 8, 15))); // Liberation Day
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 10, 3))); // National Foundation Day
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 10, 9))); // Hangul Proclamation Day

            // Lunar New Year substitute holidays
            if (lunarToSolar(LocalDate.of(year, 1, 1)).getDayOfWeek().getValue() == LD_SUNDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 3)));
            }
            if (lunarToSolar(LocalDate.of(year, 1, 1)).getDayOfWeek().getValue() == LD_MONDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 3)));
            }
            if (lunarToSolar(LocalDate.of(year, 1, 2)).getDayOfWeek().getValue() == LD_SUNDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 3)));
            }

            // Chuseok substitute holidays
            if (lunarToSolar(LocalDate.of(year, 8, 14)).getDayOfWeek().getValue() == LD_SUNDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 17)));
            }
            if (lunarToSolar(LocalDate.of(year, 8, 15)).getDayOfWeek().getValue() == LD_SUNDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 17)));
            }
            if (lunarToSolar(LocalDate.of(year, 8, 16)).getDayOfWeek().getValue() == LD_SUNDAY) {
                holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 17)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        map.put(year, holidaysSet);
        return holidaysSet;
    }

    private LocalDate substituteHoliday(LocalDate h) {
        if (h.getDayOfWeek().getValue() == LD_SUNDAY) {
            return h.plusDays(1);
        }
        if (h.getDayOfWeek().getValue() == LD_SATURDAY) {
            return h.plusDays(2);
        }
        return h;
    }

    public boolean isHoliday(LocalDate date) {
        int year = date.getYear();
        Set<LocalDate> holidays = holidaySet(year);
        return holidays.contains(date);
    }
}
