package com.jurosys.extension.com;

public class Holiday {
    private int year;
    private int month;
    private int day;

    public Holiday(int year, int month, int day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Holiday holiday = (Holiday) obj;
        return year == holiday.year && month == holiday.month && day == holiday.day;
    }

    @Override
    public int hashCode() {
        return 31 * year + 31 * month + day;
    }
}
