package org.terry.elasticsearch.query.constant;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public enum DateHistogramIntervalEnum {
    t30s(DateHistogramInterval.seconds(30)),
    t1m(DateHistogramInterval.minutes(1)),
    t2m(DateHistogramInterval.minutes(2)),
    t5m(DateHistogramInterval.minutes(5)),
    t10m(DateHistogramInterval.minutes(10)),
    t15m(DateHistogramInterval.minutes(15)),
    t30m(DateHistogramInterval.minutes(30)),
    t1h(DateHistogramInterval.hours(1)),
    t2h(DateHistogramInterval.hours(2)),
    t3h(DateHistogramInterval.hours(3)),
    t6h(DateHistogramInterval.hours(6)),
    t12h(DateHistogramInterval.hours(12)),
    t1d(DateHistogramInterval.days(1)),
    t2d(DateHistogramInterval.days(2)),
    t3d(DateHistogramInterval.days(3)),
    t1w(DateHistogramInterval.weeks(1));

    private DateHistogramInterval interval;

    public DateHistogramInterval getInterval() {
        return interval;
    }

    public void setInterval(DateHistogramInterval interval) {
        this.interval = interval;
    }

    private DateHistogramIntervalEnum(DateHistogramInterval interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return this.interval.toString();
    }
}
