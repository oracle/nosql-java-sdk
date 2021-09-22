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

import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.StatsConfig;
import oracle.nosql.driver.ThrottlingException;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.query.PlanIter;
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
    private ExtraQueryStats extraQueryStats;

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
        private int retryDelayMs = 0;
        private int rateLimitDelayMs = 0;
        private int networkLatencyMin = Integer.MAX_VALUE;
        private int networkLatencyMax = 0;
        private long networkLatencySum = 0;
        private Percentile wireLatencyPercentile;

        synchronized void observe(boolean error, int retries, int retryDelay,
            int rateLimitDelay, int authCount, int throttleCount, int reqSize,
            int resSize, int networkLatency) {

            this.count++;
            this.retryCount += retries;
            this.retryDelayMs += retryDelay;
            this.retryAuthCount += authCount;
            this.retryThrottleCount += throttleCount;
            this.rateLimitDelayMs += rateLimitDelay;

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

                this.networkLatencyMin =
                    Math.min(this.networkLatencyMin, networkLatency);
                this.networkLatencyMax =
                    Math.max(this.networkLatencyMax, networkLatency);
                this.networkLatencySum += networkLatency;

                if (this.wireLatencyPercentile != null) {
                    this.wireLatencyPercentile.addValue(networkLatency);
                }
            }
        }

        synchronized void toJSON(String requestName, ArrayValue reqArray) {
            if (count > 0) {
                MapValue mapValue = new MapValue();
                mapValue.put("name", requestName);

                toMapValue(mapValue);
                reqArray.add(mapValue);
            }
        }

        private void toMapValue(MapValue mapValue) {
            mapValue.put("count", count);
            mapValue.put("errors", errors);

            MapValue retry = new MapValue();
            retry.put("count", retryCount);
            retry.put("delayMs", retryDelayMs);
            retry.put("authCount", retryAuthCount);
            retry.put("throttleCount", retryThrottleCount);
            mapValue.put("retry", retry);
            mapValue.put("rateLimitDelayMs", rateLimitDelayMs);

            if (networkLatencyMax > 0) {
                MapValue latency = new MapValue();
                latency.put("min", networkLatencyMin);
                latency.put("max", networkLatencyMax);
                latency.put("avg",
                    1.0 * networkLatencySum / (count - errors));
                if (wireLatencyPercentile != null) {
                    latency.put("95th",
                        wireLatencyPercentile.get95thPercentile());
                    latency.put("99th",
                       wireLatencyPercentile.get99thPercentile());
                }
                mapValue.put("networkLatencyMs", latency);
            }

            if (reqSizeMax > 0) {
                MapValue reqSize = new MapValue();
                reqSize.put("min", reqSizeMin);
                reqSize.put("max", reqSizeMax);
                reqSize.put("avg", 1.0 * reqSizeSum / (count - errors));
                mapValue.put("requestSize", reqSize);
            }

            if (resSizeMax > 0) {
                MapValue resSize = new MapValue();
                resSize.put("min", resSizeMin);
                resSize.put("max", resSizeMax);
                resSize.put("avg", 1.0 * resSizeSum / (count - errors));
                mapValue.put("resultSize", resSize);
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
            retryDelayMs = 0;
            rateLimitDelayMs = 0;
            networkLatencyMin = Integer.MAX_VALUE;
            networkLatencyMax = 0;
            networkLatencySum = 0;
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

    static class ExtraQueryStats {

        static class QueryEntryStat {
            long countAPI;
            long unprepared;
            long simple;
            boolean doesWrites;
            ReqStats reqStats;
            String plan;

            QueryEntryStat(StatsConfig statsConfig, QueryRequest queryRequest) {
                reqStats = new ReqStats();

                if (statsConfig.getProfile().ordinal() >=
                    StatsConfig.Profile.MORE.ordinal()) {
                    reqStats.wireLatencyPercentile = new Percentile();
                }

                PreparedStatement pStmt = queryRequest.getPreparedStatement();
                if (pStmt != null) {
                    plan = pStmt.printDriverPlan();
                    doesWrites = pStmt.doesWrites();
                }
            }
        }

        private Map<String, QueryEntryStat> queries = new HashMap<>();
        private StatsConfig statsConfig;

        ExtraQueryStats(StatsConfig statsConfig) {
            this.statsConfig = statsConfig;
        }

        synchronized void observeQuery(QueryRequest queryRequest) {
            QueryEntryStat qStat = getExtraQueryStat(queryRequest);

            qStat.countAPI++;
            if (!queryRequest.isPrepared()) {
                qStat.unprepared++;
            }
            if (queryRequest.isPrepared() && queryRequest.isSimpleQuery()) {
                qStat.simple++;
            }

            if (queryRequest.getPreparedStatement() != null &&
                queryRequest.getPreparedStatement().driverPlan() != null) {
                PlanIter drvPlan = queryRequest.getPreparedStatement().driverPlan();
                drvPlan.getKind();
            }
        }

        synchronized void observeQuery(QueryRequest queryRequest, boolean error,
            int retries, int retryDelay, int rateLimitDelay,
            int authCount, int throttleCount, int reqSize, int resSize,
            int wireLatency) {

            QueryEntryStat qStat = getExtraQueryStat(queryRequest);

            qStat.reqStats.count++;
            if (error) {
                qStat.reqStats.errors++;
            }
            qStat.reqStats.retryCount += retries;
            qStat.reqStats.retryDelayMs += retryDelay;
            qStat.reqStats.rateLimitDelayMs += rateLimitDelay;
            qStat.reqStats.retryAuthCount += authCount;
            qStat.reqStats.retryThrottleCount += throttleCount;
            qStat.reqStats.reqSizeSum += reqSize;
            qStat.reqStats.reqSizeMin = Math.min(qStat.reqStats.reqSizeMin,
                reqSize);
            qStat.reqStats.reqSizeMax = Math.max(qStat.reqStats.reqSizeMax,
                reqSize);
            qStat.reqStats.resSizeSum += resSize;
            qStat.reqStats.resSizeMin = Math.min(qStat.reqStats.resSizeMin,
                resSize);
            qStat.reqStats.resSizeMax = Math.max(qStat.reqStats.resSizeMax,
                resSize);
            qStat.reqStats.networkLatencySum += wireLatency;
            qStat.reqStats.networkLatencyMin =
                Math.min(qStat.reqStats.networkLatencyMin, wireLatency);
            qStat.reqStats.networkLatencyMax =
                Math.max(qStat.reqStats.networkLatencyMax, wireLatency);
            if (qStat.reqStats.wireLatencyPercentile != null) {
                qStat.reqStats.wireLatencyPercentile.addValue(wireLatency);
            }
        }

        private QueryEntryStat getExtraQueryStat(
            QueryRequest queryRequest) {
            String sql = queryRequest.getStatement();

            QueryEntryStat qStat = queries.get(sql);

            if (qStat == null) {
                qStat = new QueryEntryStat(statsConfig, queryRequest);
                queries.put(sql, qStat);
            }

            if (qStat.plan == null) {
                PreparedStatement pStmt = queryRequest.getPreparedStatement();
                if (pStmt != null) {
                    qStat.plan = pStmt.printDriverPlan();
                    qStat.doesWrites = pStmt.doesWrites();
                }
            }
            return qStat;
        }

        synchronized void toJSON(MapValue root) {
            if (queries.size() > 0) {
                ArrayValue queryArr = new ArrayValue();
                root.put("queries", queryArr);

                queries.forEach((key, val) -> {

                    MapValue queryVal = new MapValue();
                    queryArr.add(queryVal);

                    queryVal.put("stmt", key == null ? "null" : key);
                    queryVal.put("countAPI", val.countAPI);
                    queryVal.put("unprepared", val.unprepared);
                    queryVal.put("simple", val.simple);
                    queryVal.put("doesWrites", val.doesWrites);
                    if (val.plan != null) {
                        queryVal.put("plan", val.plan);
                    }
                    val.reqStats.toMapValue(queryVal);
                });
            }
        }

        synchronized void clear() {
            queries.clear();
        }
    }

    Stats(StatsConfigImpl statsConfig) {
        this.statsConfig = statsConfig;

        // Fill in the stats objects
        for (String key : REQUEST_KEYS) {
            ReqStats reqStats = new ReqStats();
            requests.put(key, reqStats);

            if (statsConfig.getProfile().ordinal() >=
                StatsConfig.Profile.MORE.ordinal()) {
                reqStats.wireLatencyPercentile = new Percentile();
            }
        }

        if (statsConfig.getProfile().ordinal() >=
            StatsConfig.Profile.ALL.ordinal()) {
            extraQueryStats = new ExtraQueryStats(statsConfig);
        }

        // Setup the scheduler for interval logging
        Runnable runnable = () -> {
            try {
                logClientStats();
            } catch (RuntimeException re) {
                statsConfig.getLogger().log(Level.INFO,
                    "Stats exception: " + re.getMessage());
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
        if (extraQueryStats != null) {
            extraQueryStats.toJSON(root);
        }

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
        if (extraQueryStats != null) {
            extraQueryStats.clear();
        }

        startTime = System.currentTimeMillis();
        endTime = 0;
    }

    /**
     * Adds a new error statistic entry. When error we don't track request,
     * response sizes and latency.
     */
    void observeError(Request kvRequest,
        int connections) {
        observe(kvRequest, true, connections, -1, -1, -1);
    }

    /**
     * Adds a new statistic entry. Can be of 2 types: successful or error.
     * Request, result sizes and networkLatency are not registered for error entries.
     *
     * @param kvRequest The request object.
     * @param error Hard error, ie. return error to user.
     * @param connections The number of active connections in the pool.
     * @param reqSize Request size in bytes.
     * @param resSize Result size in bytes.
     * @param networkLatency Latency on the wire, in milliseconds, it doesn't
     *                    include retry delay or rate limit delay.
     */
    void observe(Request kvRequest, boolean error,
        int connections, int reqSize, int resSize, int networkLatency) {

        int authCount = 0, throttleCount = 0, retries = 0, retryDelay = 0;
        RetryStats retryStats = kvRequest.getRetryStats();
        if (retryStats != null) {
            authCount = retryStats.getNumExceptions(
                AuthenticationException.class);
            authCount += retryStats.getNumExceptions(
                SecurityInfoNotReadyException.class);

            throttleCount = retryStats.getNumExceptions(
                ThrottlingException.class);

            retries = retryStats.getRetries();
            retryDelay = retryStats.getDelayMs();
        }

        int rateLimitDelay = kvRequest.getRateLimitDelayedMs();

        ReqStats rStat;
        String requestClass = kvRequest.getClass().getSimpleName();
        requestClass = requestClass.endsWith("Request")
            ? requestClass.substring(0, requestClass.length() - 7) :
            requestClass;

        rStat = requests.get(requestClass);

        // This will not happen unless a new request type is added but not
        // registered in the REQUEST_KEYS array.
        if (rStat == null) {
            ReqStats newStat = new ReqStats();
            if (statsConfig.getProfile().ordinal() >=
                StatsConfig.Profile.MORE.ordinal()) {
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
            throttleCount, reqSize, resSize, networkLatency);

        connectionStats.observe(connections);

        if (extraQueryStats != null) {
            if (kvRequest instanceof QueryRequest) {
                QueryRequest queryRequest = (QueryRequest)kvRequest;

                extraQueryStats.observeQuery(queryRequest, error, retries,
                    retryDelay, rateLimitDelay, authCount, throttleCount,
                    reqSize, resSize, networkLatency);
            }
        }
    }

    public void observeQuery(QueryRequest qreq) {
        if (extraQueryStats != null) {
            extraQueryStats.observeQuery(qreq);
        }
    }

    /**
     * Shuts down the time scheduler.
     */
    void shutdown() {
        logClientStats();
        service.shutdown();
    }
}
