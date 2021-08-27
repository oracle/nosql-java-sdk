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

public class Stats {

    private ScheduledExecutorService service;
    private StatsConfigImpl statsConfig;

    private long startTime;
    private long endTime;
    private Map<String, ReqStats> requests;
    private ConnectionStats connectionStats;

    private static class ReqStats {
        long count;
        long errors;
        int reqSizeMin = Integer.MAX_VALUE;
        int reqSizeMax;
        float reqSizeAvg;
        int resSizeMin = Integer.MAX_VALUE;
        int resSizeMax;
        float resSizeAvg;
        int retryAuthCount;
        int retryThrottleCount;
        int retryTotalCount;
        int retryTotalDelay;
        int totalRateLimitDelay;
        long wireLatencyMin = Long.MAX_VALUE;
        long wireLatencyMax;
        float wireLatencyAvg;
        Percentile wireLatencyPercentile;
    }

    static class Percentile {
        List<Long> values;

        void addValue(long value) {
            if (values == null) {
                values = new ArrayList<>();
            }
            values.add(value);
        }

        long getPercentile(double percentile) {
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
    }

    private static class ConnectionStats {
        long count;
        int min = Integer.MAX_VALUE;
        int max;
        float avg;

        public void clear() {
            count = 0;
            min = Integer.MAX_VALUE;
            max = 0;
            avg = 0;
        }
    }

    Stats(StatsConfigImpl statsConfig) {
        this.statsConfig = statsConfig;

        requests = new HashMap<>();
        connectionStats = new ConnectionStats();

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    logClientStats();
                } catch (RuntimeException re) {
                    statsConfig.getLogger().log(Level.INFO,
                        re.getMessage());
                }
            }
        };

        LocalTime localTime = LocalTime.now();

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

    void logClientStats() {
        endTime = System.currentTimeMillis();
        FieldValue fvStats = generateFieldValueStats();
        String json = fvStats.toJson(statsConfig.getPrettyPrint() ?
            JsonOptions.PRETTY : null);

        /* Call user handle if configured. */
        StatsConfig.StatsHandler statsHandler =
            statsConfig.getHandler();
        if (statsHandler != null) {
            statsHandler.accept(fvStats);
        }

        /* start from scratch in the new interval */
        clearStats();
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

        if (connectionStats.count > 0) {
            MapValue connections = new MapValue();
            connections.put("min", connectionStats.min);
            connections.put("max", connectionStats.max);
            connections.put("avg", connectionStats.avg);
            root.put("connections", connections);
        }

        Set<Map.Entry<String, ReqStats>> entries;
        synchronized (requests) {
            entries = new HashSet<>(requests.entrySet());
        }

        if (entries.size() > 0) {

            ArrayValue reqArray = new ArrayValue();
            root.put("requests", reqArray);

            entries.forEach(e -> {

                String k = e.getKey();
                ReqStats v = e.getValue();

                MapValue req = new MapValue();
                req.put("name", k);
                req.put("count", v.count);
                req.put("errors", v.errors);

                MapValue retry = new MapValue();
                retry.put("totalCount", v.retryTotalCount);
                retry.put("totalDelay", v.retryTotalDelay);
                retry.put("authCount", v.retryAuthCount);
                retry.put("throttleCount", v.retryThrottleCount);
                req.put("retry", retry);
                req.put("totalRateLimitDelay", v.totalRateLimitDelay);

                if (v.wireLatencyMax > 0) {
                    MapValue latency = new MapValue();
                    latency.put("min", v.wireLatencyMin);
                    latency.put("max", v.wireLatencyMax);
                    latency.put("avg", v.wireLatencyAvg);
                    if (v.wireLatencyPercentile != null) {
                        latency
                            .put("95th",
                                v.wireLatencyPercentile.get95thPercentile());
                        latency
                            .put("99th",
                                v.wireLatencyPercentile.get99thPercentile());
                    }
                    req.put("wireLatency", latency);
                }

                if (v.reqSizeMax > 0) {
                    MapValue reqSize = new MapValue();
                    reqSize.put("min", v.reqSizeMin);
                    reqSize.put("max", v.reqSizeMax);
                    reqSize.put("avg", v.reqSizeAvg);
                    req.put("reqSize", reqSize);
                }

                if (v.resSizeMax > 0) {
                    MapValue resSize = new MapValue();
                    resSize.put("min", v.resSizeMin);
                    resSize.put("max", v.resSizeMax);
                    resSize.put("avg", v.resSizeAvg);
                    req.put("resSize", resSize);
                }

                reqArray.add(req);
            });
        }

        return root;
    }

    /**
     * Clear all collected stats.
     */
    void clearStats() {
        synchronized (requests) {
            startTime = System.currentTimeMillis();
            endTime = 0;
            requests.clear();
            connectionStats.clear();
        }
    }

    /**
     * Adds a new error statistic entry. When error we don't track request,
     * response sizes and latency.
     *
     * @param requestClass Type of request.
     * @param authCount Number of authCount errors.
     * @param throttleCount Number of throttleCount errors.
     */
    void addReqStatError(String requestClass, int retryCount,
        int retryDelay, int authCount, int throttleCount, int rateLimitDelay,
        int connections) {
        addReqStat(requestClass, true, retryCount, retryDelay, rateLimitDelay,
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
    void addReqStat(String requestClass, boolean error, int retries,
        int retryDelay, int rateLimitDelay, int authCount, int throttleCount,
        int connections, int reqSize, int resSize, long wireLatency) {

        ReqStats rStat;
        requestClass = requestClass != null && requestClass.endsWith("Request")
            ? requestClass.substring(0, requestClass.length() - 7) :
            requestClass;
        synchronized (requests) {
            rStat = requests.get(requestClass);
            if (rStat == null) {
                rStat = new ReqStats();
                requests.put(requestClass, rStat);

                if (statsConfig.getProfile() == StatsConfig.Profile.FULL) {
                    rStat.wireLatencyPercentile = new Percentile();
                }
            }
        }

        synchronized (rStat) {
            rStat.count++;
            rStat.retryTotalCount += retries;
            rStat.retryTotalDelay += retryDelay;
            rStat.retryAuthCount += authCount;
            rStat.retryThrottleCount += throttleCount;
            rStat.totalRateLimitDelay += rateLimitDelay;
            if (error) {
                rStat.errors ++;
            } else {
                if (reqSize < rStat.reqSizeMin) {
                    rStat.reqSizeMin = reqSize;
                }
                if (reqSize > rStat.reqSizeMax) {
                    rStat.reqSizeMax = reqSize;
                }

                if (resSize < rStat.resSizeMin) {
                    rStat.resSizeMin = resSize;
                }
                if (resSize > rStat.resSizeMax) {
                    rStat.resSizeMax = resSize;
                }

                if (wireLatency < rStat.wireLatencyMin) {
                    rStat.wireLatencyMin = wireLatency;
                }
                if (wireLatency > rStat.wireLatencyMax) {
                    rStat.wireLatencyMax = wireLatency;
                }

                if (rStat.wireLatencyPercentile != null) {
                    rStat.wireLatencyPercentile.addValue(wireLatency);
                }

                if (rStat.count == 0) {
                    rStat.reqSizeAvg = reqSize;
                    rStat.resSizeAvg = resSize;
                    rStat.wireLatencyAvg = wireLatency;
                } else {
                    rStat.reqSizeAvg = rStat.reqSizeAvg +
                        (reqSize - rStat.reqSizeAvg) /
                            (rStat.count - rStat.errors);

                    rStat.resSizeAvg = rStat.resSizeAvg +
                        (resSize - rStat.resSizeAvg) /
                            (rStat.count - rStat.errors);

                    rStat.wireLatencyAvg = rStat.wireLatencyAvg +
                        (wireLatency - rStat.wireLatencyAvg) /
                            (rStat.count - rStat.errors);
                }
            }
        }

        synchronized (connectionStats) {
            if (connections < connectionStats.min) {
                connectionStats.min = connections;
            }
            if (connections > connectionStats.max) {
                connectionStats.max = connections;
            }
            if (connectionStats.count == 0) {
                connectionStats.avg = connections;
            } else {
                connectionStats.avg = connectionStats.avg +
                    (connections - connectionStats.avg) /
                        connectionStats.count;
            }
            connectionStats.count++;
        }
    }

    /**
     * Shuts down the time scheduler.
     */
    void shutdown() {
        service.shutdown();
    }
}
