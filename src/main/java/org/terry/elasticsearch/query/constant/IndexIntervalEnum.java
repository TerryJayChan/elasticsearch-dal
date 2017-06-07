package org.terry.elasticsearch.query.constant;

public enum IndexIntervalEnum {
    HOUR("hour", 1L * 1000 * 60 * 60),
    DAY("day", 1L * 1000 * 60 * 60 * 24),
    WEEK("week", 1L * 1000 * 60 * 60 * 24 * 7),
    MONTH("month", 1L * 1000 * 60 * 60 * 24 * 31),
    YEAR("year", 1L * 1000 * 60 * 60 * 24 * 365);

    private long interval;
    private String name;

    private IndexIntervalEnum(String name, long interval) {
        this.name = name;
        this.interval = interval;
    }

    public long getInterval() {
        return interval;
    }

    public String getName() {
        return name;
    }
}
