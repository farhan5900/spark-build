package org.apache.spark.metrics.sink.statsd;

final class Configuration {
    final static class Keys {
        final static String HOST = "host";
        final static String PORT = "port";
        final static String PREFIX = "prefix";
        final static String TAGS = "tags";
        final static String POLL_INTERVAL = "poll.interval";
        final static String POLL_UNIT = "poll.unit";
        final static String RATE_UNIT = "rate.unit";
        final static String DURATION_UNIT = "duration.unit";
        final static String EXCLUDES = "excludes";
        final static String INCLUDES = "includes";
        final static String USE_REGEX_FILTERS = "useRegexFilters";
    }

    final static class Defaults {
        final static String HOST = "127.0.0.1";
        final static String PORT = "8125";
        final static String TAGS = "";
        final static String POLL_INTERVAL = "10";
        final static String POLL_UNIT = "SECONDS";
        final static String RATE_UNIT = "SECONDS";
        final static String DURATION_UNIT = "MILLISECONDS";
        final static String PREFIX = "";
        final static String EXCLUDES = "";
        final static String INCLUDES = "";
        final static String USE_REGEX_FILTERS = "false";
    }
}
