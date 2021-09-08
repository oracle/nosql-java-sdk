package oracle.nosql.driver.http;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.nosql.driver.StatsConfig;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;


class Stats {

    private static String[] REQUEST_KEYS = new String[] {
        "Delete", "Get", "GetIndexes", "GetTable",
        "ListTables", "MultiDelete", "Prepare", "Put", "Query", "Read",
        "System", "SystemStatus", "Table", "TableUsage",
        "WriteMultiple", "Write"};
    private ScheduledExecutorService service;
    private StatsConfigImpl statsConfig;

    private long startTime;
    private long endTime;
    private Map<String, ReqStats> requests = new HashMap<>();
    private ConnectionStats connectionStats = new ConnectionStats();

    private static class ReqStats {
        private long count = 0;
        private long errors = 0;
        private int reqSizeMin = Integer.MAX_VALUE;
        private int reqSizeMax = 0;
        private long reqSizeSum = 0;
        private int resSizeMin = Integer.MAX_VALUE;
        private int resSizeMax = 0;
        private long resSizeSum = 0;
        private int retryAuthCount = 0;
        private int retryThrottleCount = 0;
        private int retryCount = 0;
        private int retryDelay = 0;
        private int rateLimitDelay = 0;
        private int wireLatencyMin = Integer.MAX_VALUE;
        private int wireLatencyMax = 0;
        private long wireLatencySum = 0;
        private Percentile wireLatencyPercentile;

        synchronized void observe(boolean error, int retries, int retryDelay,
            int rateLimitDelay, int authCount, int throttleCount, int reqSize,
            int resSize, int wireLatency) {

            this.count++;
            this.retryCount += retries;
            this.retryDelay += retryDelay;
            this.retryAuthCount += authCount;
            this.retryThrottleCount += throttleCount;
            this.rateLimitDelay += rateLimitDelay;

            if (error) {
                this.errors++;
            }
            else {
                this.reqSizeMin = Math.min(this.reqSizeMin, reqSize);
                this.reqSizeMax = Math.max(this.reqSizeMax, reqSize);
                this.reqSizeSum += reqSize;

                this.resSizeMin = Math.min(this.resSizeMin, resSize);
                this.resSizeMax = Math.max(this.resSizeMax, resSize);
                this.resSizeSum += resSize;

                this.wireLatencyMin =
                    Math.min(this.wireLatencyMin, wireLatency);
                this.wireLatencyMax =
                    Math.max(this.wireLatencyMax, wireLatency);
                this.wireLatencySum += wireLatency;

                if (this.wireLatencyPercentile != null) {
                    this.wireLatencyPercentile.addValue(wireLatency);
                }
            }
        }

        synchronized void toJSON(String requestName, ArrayValue reqArray) {
            if (count > 0) {
                MapValue req = new MapValue();
                req.put("name", requestName);
                req.put("count", count);
                req.put("errors", errors);

                MapValue retry = new MapValue();
                retry.put("count", retryCount);
                retry.put("delay", retryDelay);
                retry.put("authCount", retryAuthCount);
                retry.put("throttleCount", retryThrottleCount);
                req.put("retry", retry);
                req.put("rateLimitDelay", rateLimitDelay);

                if (wireLatencyMax > 0) {
                    MapValue latency = new MapValue();
                    latency.put("min", wireLatencyMin);
                    latency.put("max", wireLatencyMax);
                    latency.put("avg",
                        1.0 * wireLatencySum / (count - errors));
                    if (wireLatencyPercentile != null) {
                        latency.put("95th",
                            wireLatencyPercentile.get95thPercentile());
                        latency.put("99th",
                           wireLatencyPercentile.get99thPercentile());
                    }
                    req.put("wireLatency", latency);
                }

                if (reqSizeMax > 0) {
                    MapValue reqSize = new MapValue();
                    reqSize.put("min", reqSizeMin);
                    reqSize.put("max", reqSizeMax);
                    reqSize.put("avg", 1.0 * reqSizeSum / (count - errors));
                    req.put("reqSize", reqSize);
                }

                if (resSizeMax > 0) {
                    MapValue resSize = new MapValue();
                    resSize.put("min", resSizeMin);
                    resSize.put("max", resSizeMax);
                    resSize.put("avg", 1.0 * resSizeSum / (count - errors));
                    req.put("resSize", resSize);
                }

                reqArray.add(req);
            }
        }

        synchronized void clear() {
            count = 0;
            errors = 0;
            reqSizeMin = Integer.MAX_VALUE;
            reqSizeMax = 0;
            reqSizeSum = 0;
            resSizeMin = Integer.MAX_VALUE;
            resSizeMax = 0;
            resSizeSum = 0;
            retryAuthCount = 0;
            retryThrottleCount = 0;
            retryCount = 0;
            retryDelay = 0;
            rateLimitDelay = 0;
            wireLatencyMin = Integer.MAX_VALUE;
            wireLatencyMax = 0;
            wireLatencySum = 0;
            if (wireLatencyPercentile != null) {
                wireLatencyPercentile.clear();
            }
        }
    }

    private static class Percentile {
        private List<Long> values;

        synchronized void addValue(long value) {
            if (values == null) {
                values = new ArrayList<>();
            }
            values.add(value);
        }

        synchronized long getPercentile(double percentile) {
            if (values == null || values.size() == 0) {
                return -1;
            }

            if (values.size() == 1) {
                return values.get(0);
            }

            values.sort(Comparator.comparingLong(Long::longValue));
            int index = (int)Math.round(percentile * values.size());
            if (index >= values.size()) {
                index = values.size() -1;
            }
            return values.get(index);
        }

        long get95thPercentile() {
            return getPercentile(0.95d);
        }

        long get99thPercentile() {
            return getPercentile(0.99d);
        }

        synchronized void clear() {
            if (values != null) {
                values.clear();
            }
        }
    }

    private static class ConnectionStats {
        private long count;
        private int min = Integer.MAX_VALUE;
        private int max;
        private long sum;

        synchronized void observe(int connections) {
            if (connections < min) {
                min = connections;
            }
            if (connections > max) {
                max = connections;
            }
            sum += connections;
            count++;
        }

        synchronized void toJSON(MapValue root) {
            if (count > 0) {
                MapValue connections = new MapValue();
                connections.put("min", min);
                connections.put("max", max);
                connections.put("avg", 1.0 * sum / count);
                root.put("connections", connections);
            }
        }

        synchronized void clear() {
            count = 0;
            min = Integer.MAX_VALUE;
            max = 0;
            sum = 0;
        }
    }


    Stats(StatsConfigImpl statsConfig) {
        this.statsConfig = statsConfig;

        // Fill in the stats objects
        for (String key : REQUEST_KEYS) {
            ReqStats reqStats = new ReqStats();
            requests.put(key, reqStats);

            if (statsConfig.getProfile() == StatsConfig.Profile.FULL) {
                reqStats.wireLatencyPercentile = new Percentile();
            }
        }

        // Setup the scheduler for interval logging
        Runnable runnable = () -> {
            try {
                logClientStats();
            } catch (RuntimeException re) {
                statsConfig.getLogger().log(Level.INFO,
                    "Stats exception:" + re.getMessage());
                re.printStackTrace();
            }
        };

        LocalTime localTime = LocalTime.now();

        // To log stats at the top of the hour, calculate delay until first
        // occurrence. Note: First interval can be smaller than the rest.
        long delay = 1000l * statsConfig.getInterval() -
            (( 1000l * 60l * localTime.getMinute() +
                1000l * localTime.getSecond() +
                localTime.getNano() / 1000000l) %
                (1000l * statsConfig.getInterval()));

        service = Executors
            .newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable,
            delay,
            1000l * statsConfig.getInterval(),
            TimeUnit.MILLISECONDS);

        startTime = System.currentTimeMillis();
    }

    private void logClientStats() {
        endTime = System.currentTimeMillis();

        FieldValue fvStats = generateFieldValueStats();
        // Start from scratch in the new interval.
        clearStats();

        // Call user handle if configured.
        StatsConfig.StatsHandler statsHandler =
            statsConfig.getHandler();
        if (statsHandler != null) {
            statsHandler.accept(fvStats);
        }

        // Output stats to logger.
        String json = fvStats.toJson(statsConfig.getPrettyPrint() ?
            JsonOptions.PRETTY : null);
        statsConfig.getLogger().log(Level.INFO, statsConfig.LOG_PREFIX + json);
    }

    private FieldValue generateFieldValueStats() {
        MapValue root = new MapValue();
        Timestamp ts = new Timestamp(startTime);
        ts.setNanos(0);
        root.put("startTime", new TimestampValue(ts));
        ts = new Timestamp(endTime);
        ts.setNanos(0);
        root.put("endTime", new TimestampValue(ts));
        root.put("clientId", new StringValue(statsConfig.getId()));

        connectionStats.toJSON(root);

        Set<Map.Entry<String, ReqStats>> entries;
        entries = new HashSet<>(requests.entrySet());

        if (entries.size() > 0) {
            ArrayValue reqArray = new ArrayValue();
            root.put("requests", reqArray);

            entries.forEach(e -> {
                String k = e.getKey();
                ReqStats v = e.getValue();
                v.toJSON(k , reqArray);
            });
        }

        return root;
    }

    /**
     * Clear all collected stats.
     */
    void clearStats() {
        for (String key : REQUEST_KEYS) {
            requests.get(key).clear();
        }
        connectionStats.clear();
        startTime = System.currentTimeMillis();
        endTime = 0;
    }

    /**
     * Adds a new error statistic entry. When error we don't track request,
     * response sizes and latency.
     *
     * @param requestClass Type of request.
     * @param authCount Number of authCount errors.
     * @param throttleCount Number of throttleCount errors.
     */
    void observeError(String requestClass, int retryCount,
        int retryDelay, int authCount, int throttleCount, int rateLimitDelay,
        int connections) {
        observe(requestClass, true, retryCount, retryDelay, rateLimitDelay,
            authCount, throttleCount, connections, -1, -1,-1);
    }

    /**
     * Adds a new statistic entry. Can be of 2 types: successful or error.
     * Request, result sizes and wireLatency are not registered for error entries.
     *
     * @param requestClass Type of request.
     * @param error Hard error, ie. return error to user.
     * @param authCount Number of authCount errors.
     * @param throttleCount Number of throttleCount errors.
     * @param reqSize Request size in bytes.
     * @param resSize Result size in bytes.
     * @param wireLatency Latency on the wire, in milliseconds, it doesn't
     *                    include retry delay or rate limit delay.
     */
    void observe(String requestClass, boolean error, int retries,
        int retryDelay, int rateLimitDelay, int authCount, int throttleCount,
        int connections, int reqSize, int resSize, int wireLatency) {

        ReqStats rStat;
        requestClass = requestClass != null && requestClass.endsWith("Request")
            ? requestClass.substring(0, requestClass.length() - 7) :
            requestClass;

        rStat = requests.get(requestClass);

        // This will not happen unless a new request type is added but not
        // registered in the REQUEST_KEYS array.
        if (rStat == null) {
            ReqStats newStat = new ReqStats();
            if (statsConfig.getProfile() == StatsConfig.Profile.FULL) {
                newStat.wireLatencyPercentile = new Percentile();
            }
            synchronized (requests) {
                rStat = requests.get(requestClass);
                if (rStat == null) {
                    requests.put(requestClass, newStat);
                    rStat = newStat;
                }
            }
        }

        rStat.observe(error, retries, retryDelay, rateLimitDelay, authCount,
            throttleCount, reqSize, resSize, wireLatency);

        connectionStats.observe(connections);
    }

    /**
     * Shuts down the time scheduler.
     */
    void shutdown() {
        service.shutdown();
    }
}
