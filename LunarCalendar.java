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
    static Map<Integer, Set<LocalDate>> map = new HashMap<>(); //static한 Map 타입 변수 선언

    //private method: lunarToSolar. lunar date를 받으면 solar date로 변환해준다.
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
        holidaysSet.add(LocalDate.of(year, 1, 1));   // 신정
        holidaysSet.add(LocalDate.of(year, 3, 1));   // 삼일절
        holidaysSet.add(LocalDate.of(year, 5, 5));   // 어린이날
        holidaysSet.add(LocalDate.of(year, 6, 6));   // 현충일
        holidaysSet.add(LocalDate.of(year, 8, 15));  // 광복절
        holidaysSet.add(LocalDate.of(year, 10, 3));  // 개천절
        holidaysSet.add(LocalDate.of(year, 10, 9));  // 한글날
        holidaysSet.add(LocalDate.of(year, 12, 25)); // 성탄절

        // Lunar holidays
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 1)).minusDays(1));  // 설날 전날
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 1)));               // 설날
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 1, 2)));               // 설날 다음날
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 4, 8)));               // 석가탄신일
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 14)));              // 추석 전날
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 15)));              // 추석
        holidaysSet.add(lunarToSolar(LocalDate.of(year, 8, 16)));              // 추석 다음날

        try {
            // Substitute holidays
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 5, 5)));  // 어린이날
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 3, 1)));  // 삼일절
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 8, 15))); // 광복절
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 10, 3))); // 개천절
            holidaysSet.add(substituteHoliday(LocalDate.of(year, 10, 9))); // 한글날

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
