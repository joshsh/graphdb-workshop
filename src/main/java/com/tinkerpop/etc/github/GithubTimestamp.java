package com.tinkerpop.etc.github;

import java.io.File;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class GithubTimestamp implements Comparable<GithubTimestamp> {
    private final Calendar date;

    private GithubTimestamp(final Calendar date) {
        this.date = date;
    }

    public GithubTimestamp(final String s) {
        int i;
        String[] a;
        try {
            i = s.lastIndexOf("-");
            a = s.substring(0, i).split("-");
        } catch (Exception e) {
            throw new IllegalArgumentException("not a valid timestamp: " + s);
        }

        date = new GregorianCalendar();
        date.setTimeZone(TimeZone.getTimeZone("UTC"));
        date.set(Calendar.YEAR, Integer.valueOf(a[0]));
        date.set(Calendar.MONTH, Integer.valueOf(a[1]) - 1);
        date.set(Calendar.DAY_OF_MONTH, Integer.valueOf(a[2]));

        int hour = Integer.valueOf(s.substring(i + 1));
        date.set(Calendar.HOUR_OF_DAY, hour);

        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
    }

    public GithubTimestamp(final File f) {
        this(f.getName().substring(0, f.getName().indexOf(".")));
    }

    public int compareTo(final GithubTimestamp other) {
        return this.date.compareTo(other.date);
    }

    public GithubTimestamp nextHour() {
        Calendar cal = (Calendar) date.clone();
        cal.add(Calendar.HOUR, 1);
        return new GithubTimestamp(cal);
    }

    @Override
    public String toString() {
        return date.get(Calendar.YEAR)
                + "-" + pad(1 + date.get(Calendar.MONTH))
                + "-" + pad(date.get(Calendar.DAY_OF_MONTH))
                + "-" + date.get(Calendar.HOUR_OF_DAY);
    }

    public long getTime() {
        return date.getTime().getTime();
    }

    private String pad(final int i) {
        return i < 10 ? "0" + i : "" + i;
    }
}
