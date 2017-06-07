package org.terry.elasticsearch.query.essql;

import org.terry.elasticsearch.query.constant.DateHistogramIntervalEnum;
import org.terry.elasticsearch.query.constant.IndexIntervalEnum;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

public class ESSQLUtils {

    public static String createDateHistogram(String field, String interval) {
        return createDateHistogram(field, interval, "");
    }

    public static String createDateHistogram(String field, String interval, String alias) {
        return createDateHistogram(field, interval, alias, "");
    }

    public static String createDateHistogram(String field, String interval, String alias, String timezone) {
        if (StringUtils.isBlank(field)) {
            field = "logTime";
        }
        if (StringUtils.isBlank(interval)) {
            interval = DateHistogramIntervalEnum.t1h.toString();
        }
        if (StringUtils.isBlank(alias)) {
            alias = "dateTime";
        }
        if (StringUtils.isBlank(timezone)) {
            timezone = "Asia/Shanghai";
        }
        String pattern = "date_histogram(field='%s','interval'='%s','alias'='%s', 'time_zone'='%s')";
        return String.format(pattern, field, interval, alias, timezone);
    }

    public static String createBetweenNumbers(String field, String start, String end) {
        String pattern = "%s BETWEEN %s AND %s";
        return String.format(pattern, field, start, end);
    }

    public static String createBetweenStrings(String field, String start, String end) {
        String pattern = "%s BETWEEN '%s' AND '%s'";
        return String.format(pattern, field, start, end);
    }

    public static String createInStrings(String field, String... values) {
        String pattern = "%s IN (%s)";
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append("'");
            sb.append(value);
            sb.append("'");
            sb.append(",");
        }
        String vals = StringUtils.substringBeforeLast(sb.toString(), ",");
        return String.format(pattern, field, vals);
    }

    public static String createInNumbers(String field, String... values) {
        String pattern = "%s IN (%s)";
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append(value);
            sb.append(",");
        }
        String vals = StringUtils.substringBeforeLast(sb.toString(), ",");
        return String.format(pattern, field, vals);
    }

    public static String createIndexes(String indexPrefix, String indexType, IndexIntervalEnum interval, Date
            startTime, Date endTime) {
        if (startTime == null) {
            startTime = new Date();
        }
        if (endTime == null) {
            endTime = new Date();
        }
        if (endTime.before(startTime)) {
            endTime = startTime;
        }
        if (StringUtils.isBlank(indexPrefix)) {
            indexPrefix = "dsa-nginx";
        }
        if (interval == null) {
            interval = IndexIntervalEnum.HOUR;
        }
        Set<String> sets = new LinkedHashSet<>(16);
        String endIndex = getIndex(indexPrefix, indexType, interval, new DateTime(endTime));
        sets.add(endIndex);
        while (endTime.after(startTime)) {
            sets.add(getIndex(indexPrefix, indexType, interval, new DateTime(startTime)));
            startTime = new Date(startTime.getTime() + interval.getInterval());
        }
        return StringUtils.join(sets, ",");
    }

    public static String createInterval(Date startTime, Date endTime) {
        if (startTime == null || endTime == null) {
            return DateHistogramIntervalEnum.t1h.toString();
        }
        if (endTime.before(startTime)) {
            return DateHistogramIntervalEnum.t1h.toString();
        }

        DateHistogramIntervalEnum intervalEnum;
        long timeSpan = endTime.getTime() - startTime.getTime();
        if (timeSpan <= 15 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t30s;
        } else if (timeSpan <= 30 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t1m;
        } else if (timeSpan <= 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t2m;
        } else if (timeSpan <= 6 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t15m;
        } else if (timeSpan <= 12 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t30m;
        } else if (timeSpan <= 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t1h;
        } else if (timeSpan <= 3 * 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t3h;
        } else if (timeSpan <= 7 * 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t6h;
        } else if (timeSpan <= 31 * 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t1d;
        } else if (timeSpan <= 2 * 31 * 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t2d;
        } else if (timeSpan <= 3 * 31 * 24 * 60 * 60) {
            intervalEnum = DateHistogramIntervalEnum.t3d;
        } else {
            intervalEnum = DateHistogramIntervalEnum.t1w;
        }
        return intervalEnum.toString();
    }

    private static DateTimeFormatter getIndexPattern(IndexIntervalEnum intervalEnum) {
        DateTimeFormatter formatter;
        switch (intervalEnum.getName()) {
            case "hour":
                formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH");
                break;
            case "day":
                formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
                break;
            case "week":
                formatter = ISODateTimeFormat.weekyearWeek();
                break;
            case "month":
                formatter = DateTimeFormat.forPattern("yyyy-MM");
                break;
            case "year":
                formatter = DateTimeFormat.forPattern("yyyy");
                break;
            default:
                formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
                break;
        }
        return formatter;
    }

    private static String getIndex(String indexPrefix, String indexType, IndexIntervalEnum interval, DateTime time) {
        StringBuilder sb = new StringBuilder();
        sb.append(indexPrefix).append("-").append(getIndexPattern(interval).print(time));
        if (StringUtils.isNotBlank(indexType)) {
            sb.append("/").append(indexType);
        }
        return sb.toString();
    }
}
