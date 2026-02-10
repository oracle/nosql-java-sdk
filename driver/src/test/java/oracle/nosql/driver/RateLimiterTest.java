/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.TimeUnit;

import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.util.SimpleRateLimiter;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

/**
 * Tests for driver-side rate limiting. These tests require a
 * Cloud Simulator instance as rate limiting is not available or
 * need on-premise.
 */
public class RateLimiterTest extends ProxyTestBase {

    @Test
    public void basicInternalTest() throws Exception {
        testLimiters(false, 500, 200, 200, 10, 100.0);
    }

    @Test
    public void basicExternalTest() throws Exception {
        testLimiters(true, 500, 200, 200, 10, 100.0);
    }

    @Test
    public void basicInternalPercentTest() throws Exception {
        testLimiters(false, 500, 200, 200, 10, 20.0);
    }

    @Test
    public void basicExternalPercentTest() throws Exception {
        testLimiters(true, 500, 200, 200, 10, 20.0);
    }

    @Test
    public void retryStatsTest() throws Exception {
        /* fails in jenkins because DRL is enabled by default */
        assumeTrue(inJenkins == false);
        testRetryStats(500, 500, 500, 20);
    }

    private void testRetryStats(int maxRows,
                                int readLimit,
                                int writeLimit,
                                int testSeconds) {

        assumeTrue(onprem == false);

        final boolean verbose = Boolean.getBoolean("test.verbose");

        /* clear any previous rate limiters */
        Client client = ((NoSQLHandleImpl)handle).getClient();
        client.enableRateLimiting(false, 100.0);

        /*
         * With these settings, we should get many internal throttling
         * errors. This is on purpose, to verify that retry stats are
         * properly returned in both QueryRequest and QueryResult objects.
         */
        runLimitedOpsOnTable(readLimit, writeLimit, testSeconds,
            maxRows, 100.0, verbose, false, true);
    }

    private void testLimiters(boolean useExternalLimiters,
                              int maxRows,
                              int readLimit,
                              int writeLimit,
                              int testSeconds,
                              double usePercent) {

        assumeTrue(onprem == false);

        final boolean verbose = Boolean.getBoolean("test.verbose");

        /* clear any previous rate limiters */
        Client client = ((NoSQLHandleImpl)handle).getClient();
        client.enableRateLimiting(false, 100.0);

        /* configure our handle for rate limiting */
        if (useExternalLimiters == false) {
            client.enableRateLimiting(true, usePercent);
        }

        /* limit bursts in tests */
        System.setProperty("test.rldurationsecs", "1");

        /* then do the actual testing */
        runLimitedOpsOnTable(readLimit, writeLimit, testSeconds,
            maxRows, usePercent, verbose, useExternalLimiters, false);
    }

    @Test
    public void extendedInternalFullTest() throws Exception {
        /* Skip unless extended tests are enabled */
        assumeTrue(Boolean.getBoolean("test.extended"));

        int[] allunits = new int[] {1, 50, 2000};
        for (int units : allunits) {
            testLimiters(false, 500, units, units, 10, 100.0);
        }
    }

    @Test
    public void extendedInternalPercentTest() throws Exception {
        /* Skip unless extended tests are enabled */
        assumeTrue(Boolean.getBoolean("test.extended"));

        int[] allunits = new int[] {10, 100, 2000};
        for (int units : allunits) {
            testLimiters(false, 500, units, units, 10, 10.0);
        }
    }

    @Test
    public void extendedExternalFullTest() throws Exception {
        /* Skip unless extended tests are enabled */
        assumeTrue(Boolean.getBoolean("test.extended"));

        int[] allunits = new int[] {1, 50, 2000};
        for (int units : allunits) {
            testLimiters(true, 500, units, units, 10, 100.0);
        }
    }

    @Test
    public void extendedExternalPercentTest() throws Exception {
        /* Skip unless extended tests are enabled */
        assumeTrue(Boolean.getBoolean("test.extended"));

        int[] allunits = new int[] {10, 100, 2000};
        for (int units : allunits) {
            testLimiters(true, 500, units, units, 10, 10.0);
        }
    }

    /**
     * Runs puts and gets continuously for N seconds.
     *
     * Verify that the resultant RUs/WUs used match the
     * given rate limits.
     */
    private void doRateLimitedOps(int numSeconds,
        int readLimit, int writeLimit, int maxRows,
        boolean checkUnits, double usePercent, boolean verbose,
        boolean useExternalLimiters, boolean skipAllLimiting) {

        if (readLimit == 0 && writeLimit == 0) {
            return;
        }

        PutRequest putRequest = new PutRequest()
            .setTableName("testusersRateLimit");
        GetRequest getRequest = new GetRequest()
            .setTableName("testusersRateLimit");
        MapValue key = new MapValue();

        /* TODO: random sizes 0-nKB */
        MapValue value = new MapValue().put("name", "jane");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (numSeconds * 1000);

        int readUnitsUsed = 0;
        int writeUnitsUsed = 0;

        int totalDelayedMs = 0;
        int throttleExceptions = 0;

        RateLimiter rlim = null;
        RateLimiter wlim = null;

        double maxRVal = (double)readLimit + (double)writeLimit;

        if (verbose) System.out.println("Running gets/puts: RUs=" +
            readLimit + " WUs=" + writeLimit +
            " percent=" + usePercent);

        if (skipAllLimiting == false) {
            if (useExternalLimiters == false) {
                /* reset internal limiters so they don't have unused units */
                ((NoSQLHandleImpl)handle).getClient()
                    .resetRateLimiters("testusersRateLimit");
            } else {
                rlim = new SimpleRateLimiter(
                    (readLimit * usePercent) / 100.0, 1);
                wlim = new SimpleRateLimiter(
                    (writeLimit * usePercent) / 100.0, 1);
            }
        }

        boolean doPut;

        do {
            int id = (int)(Math.random() * maxRows);
            if (readLimit == 0) {
                doPut = true;
            } else if (writeLimit == 0) {
                doPut = false;
            } else {
                int v = (int)(Math.random() * maxRVal);
                doPut = (v >= readLimit);
            }
            try {
                if (doPut) {
                    value.put("id", id);
                    putRequest.setValue(value);
                    putRequest.setReadRateLimiter(null);
                    putRequest.setWriteRateLimiter(wlim);
                    PutResult pres = handle.put(putRequest);
                    writeUnitsUsed += pres.getWriteUnits();
                    totalDelayedMs += pres.getRateLimitDelayedMs();
                    RetryStats rs = pres.getRetryStats();
                    if (rs != null) {
                        throttleExceptions +=
                            rs.getNumExceptions(WriteThrottlingException.class);
                    }
                } else {
                    key.put("id", id);
                    getRequest.setKey(key);
                    getRequest.setReadRateLimiter(rlim);
                    getRequest.setWriteRateLimiter(null);
                    GetResult gres = handle.get(getRequest);
                    readUnitsUsed += gres.getReadUnits();
                    totalDelayedMs += gres.getRateLimitDelayedMs();
                    RetryStats rs = gres.getRetryStats();
                    if (rs != null) {
                        throttleExceptions +=
                            rs.getNumExceptions(ReadThrottlingException.class);
                    }
                }
            /* we should not get throttling exceptions */
            } catch (WriteThrottlingException wte) {
                if (skipAllLimiting == false) {
                    fail("Expected no write throttling exceptions, got one");
                }
            } catch (ReadThrottlingException rte) {
                if (skipAllLimiting == false) {
                    fail("Expected no read throttling exceptions, got one");
                }
            }
        } while (System.currentTimeMillis() < endTime);

        numSeconds = (int)((System.currentTimeMillis() - startTime) / 1000);

        int RUs = readUnitsUsed / numSeconds;
        int WUs = writeUnitsUsed / numSeconds;

        if (verbose) System.out.println("Resulting RUs=" + RUs +
            " and WUs=" + WUs);
        if (verbose) System.out.println("Rate delayed time = " +
            totalDelayedMs + "ms");
        if (verbose) System.out.println("Internal throttle exceptions = " +
            throttleExceptions);

        if (checkUnits == false || skipAllLimiting == true) {
            return;
        }

        usePercent = usePercent / 100.0;

        if (RUs < (int)(readLimit * usePercent * 0.8) ||
            RUs > (int)(readLimit * usePercent * 1.2)) {
            fail("Gets: Expected around " + readLimit * usePercent +
                " RUs, got " + RUs);
        }
        if (WUs < (int)(writeLimit * usePercent * 0.8) ||
            WUs > (int)(writeLimit * usePercent * 1.2)) {
            fail("Puts: Expected around " + writeLimit * usePercent +
                " WUs, got " + WUs);
        }

    }

    /**
     * Runs queries continuously for N seconds.
     *
     * Verify that the resultant RUs used match the
     * given rate limit.
     */
    private void doRateLimitedQueries(int numSeconds,
        int readLimit, int maxKB,
        boolean singlePartition, boolean doSort, double usePercent,
        boolean verbose, boolean useExternalLimiters,
        boolean skipAllLimiting) {

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (numSeconds * 1000);

        int readUnitsUsed = 0;
        int requestDelayedMs = 0;
        int responseDelayedMs = 0;
        RetryStats requestRetryStats = new RetryStats();
        RetryStats responseRetryStats = new RetryStats();
        int totalRecords = 0;

        RateLimiter rlim = null;
        RateLimiter wlim = null;

        if (skipAllLimiting == false ) {
            if (useExternalLimiters == false) {
                /* reset internal limiters so they don't have unused units */
                ((NoSQLHandleImpl)handle).getClient()
                    .resetRateLimiters("testusersRateLimit");
            } else {
                rlim = new SimpleRateLimiter(
                    (readLimit * usePercent) / 100.0, 1);
                wlim = new SimpleRateLimiter(
                    (readLimit * usePercent) / 100.0, 1);
            }
        }

        PrepareRequest prepReq = new PrepareRequest();
        String statement;
        if (singlePartition) {
            /* Query based on single partition scanning */
            int id = (int)(Math.random() * 500.0);
            statement = "select * from testusersRateLimit " +
                "where id = " + id;
        } else {
            /* Query based on all partitions scanning */
            statement = "select * from testusersRateLimit " +
                "where name = \"jane\"";
        }
        if (doSort) {
            statement = statement + " order by name";
        }
        prepReq.setStatement(statement);
        PrepareResult prepRes = handle.prepare(prepReq);
        assertTrue("Prepare statement failed",
            prepRes.getPreparedStatement() != null);
        readUnitsUsed += prepRes.getReadUnits();

        if (maxKB <= 0) {
            maxKB = (int)((readLimit * usePercent)/100.0);
        }

        if (verbose) System.out.println("Running queries: statement=" +
            statement + "; RUs=" +
            readLimit + " percent=" + usePercent + " maxKB=" + maxKB +
            " singlePartition=" + singlePartition);

        do {
            /*
             * we need a 20 second timeout because in some cases this
             * is called on a table with 500 rows and 50RUs
             * (uses 1000RUs = 20 seconds)
             */
            try (QueryRequest queryReq = new QueryRequest()) {
                queryReq.setPreparedStatement(prepRes)
                    .setTimeout(20000)
                    .setMaxReadKB(maxKB);
                queryReq.setReadRateLimiter(rlim);
                queryReq.setWriteRateLimiter(wlim);
                try {
                    do {
                        QueryResult res = handle.query(queryReq);
                        totalRecords += res.getResults().size();
                        readUnitsUsed += res.getReadUnits();
                        requestDelayedMs += queryReq.getRateLimitDelayedMs();
                        responseDelayedMs += res.getRateLimitDelayedMs();
                        requestRetryStats.addStats(queryReq.getRetryStats());
                        responseRetryStats.addStats(res.getRetryStats());
                    }
                    while (!queryReq.isDone());
                } catch (ReadThrottlingException rte) {
                    if (skipAllLimiting == false) {
                        fail("Expected no throttling exceptions, got one");
                    }
                } catch (RequestTimeoutException te) {
                    /* this may happen for very small limit tests */
                }
                /*
                 * verify that rate limiters were used
                 */
                if (skipAllLimiting == false && useExternalLimiters == false) {
                    RateLimiter rl = queryReq.getReadRateLimiter();
                    if (rl == null) {
                        fail("query did not use rate limiter");
                    }
                }
            }
        } while (endTime > System.currentTimeMillis());

        int numMs = (int)(System.currentTimeMillis() - startTime);

        usePercent = usePercent / 100.0;

        int RUs = (readUnitsUsed * 1000) / numMs;

        if (verbose) {
            System.out.println("Total read units=" + readUnitsUsed +
                               " in " + numMs + "ms");
            System.out.println("Total records=" + totalRecords);
            System.out.println("Resulting query RUs=" + RUs);
            System.out.println("Rate limiting delayed execution by " +
                               requestDelayedMs + "ms");
            System.out.println("Request retries: " + requestRetryStats);
        }
        if (skipAllLimiting == false &&
            requestDelayedMs <= 0 && responseDelayedMs <= 0) {
            fail("Query did not delay at all due to rate limiting");
        }
        /* the delayed time should be the same in request and response */
        if (requestDelayedMs != responseDelayedMs) {
            fail("Mismatch in rate limit delay reported by request versus " +
                 "response: request=" + requestDelayedMs + " response=" +
                 responseDelayedMs);
        }
        /* retry stats should be the same as well */
        if (requestRetryStats.equals(responseRetryStats) == false) {
            fail("Mismatch in retry stats reported by request versus " +
                 "response: request=" + requestRetryStats + " response=" +
                 responseRetryStats);
        }
        /* if no limiting, retries should be nonzero */
        if (skipAllLimiting == true && requestRetryStats.getRetries() == 0) {
            fail("Expected to get internal retries, but got none");
        }

        if (skipAllLimiting == true) {
            return;
        }

        int expectedRUs = (int)(readLimit * usePercent);

        /* for very small expected amounts, just verify within 1 RU */
        if (expectedRUs < 4 &&
            RUs <= (expectedRUs + 1) &&
            RUs >= (expectedRUs - 1)) {
            return;
        }

        if (RUs < (int)(expectedRUs * 0.6) ||
            RUs > (int)(expectedRUs * 1.5)) {
            fail("Query: \"" + prepReq.getStatement() +
                 "\"\nExpected around " + expectedRUs + " RUs, got " + RUs);
        }
    }

    private void ensureTableExistsWithLimits(int readLimit, int writeLimit) {
        try {
            alterTableLimits(handle,
                             "testusersRateLimit",
                             new TableLimits(readLimit, writeLimit, 50));
            return;
        } catch (TableNotFoundException tnfe) {}

        createAndPopulateTable();

        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (Exception e) {}

        alterTableLimits(handle,
                         "testusersRateLimit",
                         new TableLimits(readLimit, writeLimit, 50));
    }

    private void createAndPopulateTable() {
        TableResult tres = tableOperation(
            handle,
            "create table if not exists testusersRateLimit(id integer, " +
            "name string, primary key(id))",
            new TableLimits(50000, 50000, 50));
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* fill table with data */
        doRateLimitedOps(
            5 /* seconds */,
            50000, 50000, /* r/w limits */
            500, /* maxRows */
            false /* don't check resulting rate */,
            100.0 /* usePercent */,
            verbose,
            false /* use internal limiting */,
            true /* skip all limiting */);
    }

    /**
     * Runs get/puts then queries on a table.
     * Verify RUs/WUs are within given limits.
     */
    private void runLimitedOpsOnTable(
        int readLimit, int writeLimit, int maxSeconds, int maxRows,
        double usePercent, boolean verbose, boolean useExternalLimiters,
        boolean skipAllLimiting) {

        /* TODO: test large versus small records */

        if (verbose) {
            System.out.println("Running rate limiting test: RUs=" +
                readLimit + " WUs=" + writeLimit + " usePercent=" +
                usePercent + " external=" + useExternalLimiters);
        }

        ensureTableExistsWithLimits(readLimit, writeLimit);

        /*
         * we have to do the read/write ops separately since we're
         * running single-threaded, and the result is hard to tell
         * if it's correct (example: we'd get 37RUs and 15WUs)
         */
        doRateLimitedOps(maxSeconds, 0, writeLimit,
            maxRows, true, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
        doRateLimitedOps(maxSeconds, readLimit, 0,
            maxRows, true, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);

        /* Query based on single partition scanning, no sort */
        doRateLimitedQueries(maxSeconds, readLimit,
            20, true, false, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
        /* Query based on single partition scanning, with sort */
        doRateLimitedQueries(maxSeconds, readLimit,
            20, true, true, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
        /* Query based on all partitions scanning - no sort */
        doRateLimitedQueries(maxSeconds, readLimit,
            20, false, false, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
        /* Query based on all partitions scanning - with sort */
        doRateLimitedQueries(maxSeconds, readLimit,
            20, false, true, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
        /* Query based on all partitions scanning, no limit per req */
        doRateLimitedQueries(maxSeconds, readLimit,
            0, false, true, usePercent, verbose, useExternalLimiters,
            skipAllLimiting);
    }
}
