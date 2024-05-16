package com.jurosys.extension.com;

import java.time.LocalDate;

public class TestHoliday {
    public static void main(String[] args) {
        LunarCalendar lunarCalendar = new LunarCalendar();

        LocalDate dateToCheck = LocalDate.of(2025, 01, 28);
        boolean isHoliday = lunarCalendar.isHoliday(dateToCheck);

        System.out.println("Is " + dateToCheck + " a holiday? " + isHoliday);
    }
}
