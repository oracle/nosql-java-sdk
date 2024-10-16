/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import java.io.PrintWriter;
import java.io.StringWriter;
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
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.ThrottlingException;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;

/**
 * Stores and logs all collected statistics.
 */
public class Stats {

    // The following keys must match the request names.
    private static String[] REQUEST_KEYS = new String[] {
        "Delete", "Get", "GetIndexes", "GetTable",
        "ListTables", "MultiDelete", "Prepare", "Put", "Query",
        "System", "SystemStatus", "Table", "TableUsage",
        "WriteMultiple", "Write"};
    private ScheduledExecutorService service;
    private StatsControlImpl statsControl;

    private long startTime;
    private long endTime;
    private Map<String, ReqStats> requests = new HashMap<>();
    private ConnectionStats connectionStats = new ConnectionStats();
    private ExtraQueryStats extraQueryStats;

    /**
     * Stores per type of request statistics.
     */
    private static class ReqStats {
        private long httpRequestCount = 0;
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
        private int requestLatencyMin = Integer.MAX_VALUE;
        private int requestLatencyMax = 0;
        private long requestLatencySum = 0;
        private Percentile requestLatencyPercentile;

        synchronized void observe(boolean error, int retries, int retryDelay,
            int rateLimitDelay, int authCount, int throttleCount, int reqSize,
            int resSize, int requestLatency) {

            this.httpRequestCount++;
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

                this.requestLatencyMin =
                    Math.min(this.requestLatencyMin, requestLatency);
                this.requestLatencyMax =
                    Math.max(this.requestLatencyMax, requestLatency);
                this.requestLatencySum += requestLatency;

                if (this.requestLatencyPercentile != null) {
                    this.requestLatencyPercentile.addValue(requestLatency);
                }
            }
        }

        synchronized void toJSON(String requestName, ArrayValue reqArray) {
            if (httpRequestCount > 0) {
                MapValue mapValue = new MapValue();
                mapValue.put("name", requestName);

                toMapValue(mapValue);
                reqArray.add(mapValue);
            }
        }

        private void toMapValue(MapValue mapValue) {
            mapValue.put("httpRequestCount", httpRequestCount);
            mapValue.put("errors", errors);

            MapValue retry = new MapValue();
            retry.put("count", retryCount);
            retry.put("delayMs", retryDelayMs);
            retry.put("authCount", retryAuthCount);
            retry.put("throttleCount", retryThrottleCount);
            mapValue.put("retry", retry);
            mapValue.put("rateLimitDelayMs", rateLimitDelayMs);

            if (requestLatencyMax > 0) {
                MapValue latency = new MapValue();
                latency.put("min", requestLatencyMin);
                latency.put("max", requestLatencyMax);
                latency.put("avg", 1.0 * requestLatencySum /
                    (httpRequestCount - errors));
                if (requestLatencyPercentile != null) {
                    latency.put("95th",
                        requestLatencyPercentile.get95thPercentile());
                    latency.put("99th",
                       requestLatencyPercentile.get99thPercentile());
                }
                mapValue.put("httpRequestLatencyMs", latency);
            }

            if (reqSizeMax > 0) {
                MapValue reqSize = new MapValue();
                reqSize.put("min", reqSizeMin);
                reqSize.put("max", reqSizeMax);
                reqSize.put("avg", 1.0 * reqSizeSum /
                    (httpRequestCount - errors));
                mapValue.put("requestSize", reqSize);
            }

            if (resSizeMax > 0) {
                MapValue resSize = new MapValue();
                resSize.put("min", resSizeMin);
                resSize.put("max", resSizeMax);
                resSize.put("avg", 1.0 * resSizeSum /
                    (httpRequestCount - errors));
                mapValue.put("resultSize", resSize);
            }
        }

        synchronized void clear() {
            httpRequestCount = 0;
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
            requestLatencyMin = Integer.MAX_VALUE;
            requestLatencyMax = 0;
            requestLatencySum = 0;
            if (requestLatencyPercentile != null) {
                requestLatencyPercentile.clear();
            }
        }
    }

    /**
     * Percentile class helps storing and calculating percentiles.
     */
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

            values.sort(Comparator.comparingLong(Long::longValue));
            int index = (int)Math.round(percentile * values.size() - 1);

            if (index < 0) {
                index = 0;
            }
            if (index >= values.size()) {
                index = values.size() - 1;
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

    /**
     * Stores connection aggregated statistics. Min, max, avg show the number of
     * simultaneously opened connections.
     */
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

    /**
     * Stores more detailed per query statistics.
     */
    static class ExtraQueryStats {

        /**
         * Stores statistics for a query type.
         */
        static class QueryEntryStat {
            long count;
            long unprepared;
            boolean simple;
            boolean doesWrites;
            ReqStats reqStats;
            String plan;

            QueryEntryStat(StatsControl statsConfig, QueryRequest queryRequest) {
                reqStats = new ReqStats();

                if (statsConfig.getProfile().ordinal() >=
                    StatsControl.Profile.MORE.ordinal()) {
                    reqStats.requestLatencyPercentile = new Percentile();
                }

                PreparedStatement pStmt = queryRequest.getPreparedStatement();
                if (pStmt != null) {
                    plan = pStmt.printDriverPlan();
                    doesWrites = pStmt.doesWrites();
                }
            }
        }

        private Map<String, QueryEntryStat> queries = new HashMap<>();
        private StatsControl statsConfig;

        ExtraQueryStats(StatsControl statsConfig) {
            this.statsConfig = statsConfig;
        }

        synchronized void observeQuery(QueryRequest queryRequest) {
            QueryEntryStat qStat = getExtraQueryStat(queryRequest);

            qStat.count++;
            if (!queryRequest.isPrepared()) {
                qStat.unprepared++;
            } else {
                qStat.simple = queryRequest.isSimpleQuery();
            }
        }

        synchronized void observeQuery(QueryRequest queryRequest, boolean error,
            int retries, int retryDelay, int rateLimitDelay,
            int authCount, int throttleCount, int reqSize, int resSize,
            int requestLatency) {

            QueryEntryStat qStat = getExtraQueryStat(queryRequest);

            qStat.reqStats.httpRequestCount++;
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
            qStat.reqStats.requestLatencySum += requestLatency;
            qStat.reqStats.requestLatencyMin =
                Math.min(qStat.reqStats.requestLatencyMin, requestLatency);
            qStat.reqStats.requestLatencyMax =
                Math.max(qStat.reqStats.requestLatencyMax, requestLatency);
            if (qStat.reqStats.requestLatencyPercentile != null) {
                qStat.reqStats.requestLatencyPercentile.addValue(requestLatency);
            }
        }

        private QueryEntryStat getExtraQueryStat(
            QueryRequest queryRequest) {
            String sql = queryRequest.getStatement();
            if (sql == null && queryRequest.getPreparedStatement() != null) {
                sql = queryRequest.getPreparedStatement().getSQLText();
            }

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

                    queryVal.put("query", key == null ? "null" : key);
                    queryVal.put("count", val.count);
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

    Stats(StatsControlImpl statsControl) {
        this.statsControl = statsControl;

        // Fill in the stats objects
        for (String key : REQUEST_KEYS) {
            ReqStats reqStats = new ReqStats();
            requests.put(key, reqStats);

            if (statsControl.getProfile().ordinal() >=
                StatsControl.Profile.MORE.ordinal()) {
                reqStats.requestLatencyPercentile = new Percentile();
            }
        }

        if (statsControl.getProfile().ordinal() >=
            StatsControl.Profile.ALL.ordinal()) {
            extraQueryStats = new ExtraQueryStats(statsControl);
        }

        // Set up the scheduler for interval logging
        Runnable runnable = () -> {
            try {
                logClientStats();
            } catch (RuntimeException re) {
                if (statsControl.getLogger() != null) {
                    StringWriter stackTrace = new StringWriter();
                    re.printStackTrace(new PrintWriter(stackTrace));
                    statsControl.getLogger().log(Level.INFO,
                        "Stats exception: " + re.getMessage() + "\n" +
                            stackTrace);
                }
            }
        };

        LocalTime localTime = LocalTime.now();

        // To log stats at the top of the hour, calculate delay until first
        // occurrence. Note: First interval can be smaller than the rest.
        long delay = 1000L * statsControl.getInterval() -
            ((1000L * 60L * localTime.getMinute() +
                1000L * localTime.getSecond() +
                localTime.getNano() / 1000000L) %
                (1000L * statsControl.getInterval()));

        service = Executors
            .newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable,
            delay,
            1000L * statsControl.getInterval(),
            TimeUnit.MILLISECONDS);

        startTime = System.currentTimeMillis();
    }

    private void logClientStats() {
        endTime = System.currentTimeMillis();

        MapValue fvStats = generateFieldValueStats();
        // Start from scratch in the new interval.
        clearStats();

        // Call user handle if configured.
        StatsControl.StatsHandler statsHandler =
            statsControl.getStatsHandler();
        if (statsHandler != null) {
            statsHandler.accept(fvStats);
        }

        // Output stats to logger.
        if (statsControl.getLogger() != null) {
            String json = fvStats.toJson(statsControl.getPrettyPrint() ?
                JsonOptions.PRETTY : null);
            statsControl.getLogger().log(Level.INFO,
                StatsControl.LOG_PREFIX + json);
        }
    }

    private MapValue generateFieldValueStats() {
        MapValue root = new MapValue();
        Timestamp ts = new Timestamp(startTime);
        ts.setNanos(0);
        root.put("startTime", new TimestampValue(ts));
        ts = new Timestamp(endTime);
        ts.setNanos(0);
        root.put("endTime", new TimestampValue(ts));
        root.put("clientId", new StringValue(statsControl.getId()));

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
    private void clearStats() {
        for (String key : requests.keySet()) {
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
     * Request, result sizes and requestLatency are not registered for error
     * entries.
     *
     * @param kvRequest The request object.
     * @param error Hard error, ie. return error to user.
     * @param connections The number of active connections in the pool.
     * @param reqSize Request size in bytes.
     * @param resSize Result size in bytes.
     * @param requestLatency Latency on the wire, in milliseconds, it doesn't
     *                       include retry delay or rate limit delay.
     */
    void observe(Request kvRequest, boolean error,
        int connections, int reqSize, int resSize, int requestLatency) {

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
        String requestName = kvRequest.getTypeName();
        ReqStats rStat = requests.get(requestName);

        // This will not happen unless a new request type is added but not
        // registered in the REQUEST_KEYS array.
        if (rStat == null) {
            ReqStats newStat = new ReqStats();
            if (statsControl.getProfile().ordinal() >=
                StatsControl.Profile.MORE.ordinal()) {
                newStat.requestLatencyPercentile = new Percentile();
            }
            synchronized (requests) {
                rStat = requests.get(requestName);
                if (rStat == null) {
                    requests.put(requestName, newStat);
                    rStat = newStat;
                }
            }
        }

        rStat.observe(error, retries, retryDelay, rateLimitDelay, authCount,
            throttleCount, reqSize, resSize, requestLatency);

        connectionStats.observe(connections);

        if (extraQueryStats == null &&
            statsControl.getProfile().ordinal() >=
                StatsControl.Profile.ALL.ordinal()) {
            extraQueryStats = new ExtraQueryStats(statsControl);
        }

        if (extraQueryStats != null &&
            statsControl.getProfile().ordinal() >=
                StatsControl.Profile.ALL.ordinal()) {
            if (kvRequest instanceof QueryRequest) {
                QueryRequest queryRequest = (QueryRequest)kvRequest;

                extraQueryStats.observeQuery(queryRequest, error, retries,
                    retryDelay, rateLimitDelay, authCount, throttleCount,
                    reqSize, resSize, requestLatency);
            }
        }
    }

    /**
     * Adds a new statistic entry for this query request.
     */
    void observeQuery(QueryRequest qreq) {
        if (extraQueryStats == null &&
            statsControl.getProfile().ordinal() >=
            StatsControl.Profile.ALL.ordinal()) {
            extraQueryStats = new ExtraQueryStats(statsControl);
        }

        if (extraQueryStats != null &&
            statsControl.getProfile().ordinal() >=
                StatsControl.Profile.ALL.ordinal()) {
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
