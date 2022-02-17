/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

public class StatsTest extends ProxyTestBase {

    final private static String TABLE_NAME = "statsTestTable";
    // Logging of stats set to 3 seconds
    final private static int INTERVAL_SEC = 3;
    // Wait for stats to be logged a little more than 3 seconds.
    final private static int SLEEP_MSEC = INTERVAL_SEC * 1000 + 200;

    List<MapValue> statsList = null;

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
            "sid INTEGER, id INTEGER, name STRING, longString STRING, " +
            "PRIMARY KEY(SHARD(sid), id))";

    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();
        tableOperation(handle, createTableDDL,
            new TableLimits(10000, 10000, 50));
    }

    /* Set configuration values for the handle */
    @Override
    protected void perTestHandleConfig(NoSQLHandleConfig config) {
        assertNull(config.getStatsHandler());
        assertEquals(NoSQLHandleConfig.DEFAULT_STATS_INTERVAL,
            config.getStatsInterval());
        assertFalse(config.getStatsPrettyPrint());
        assertEquals(NoSQLHandleConfig.DEFAULT_STATS_PROFILE,
            config.getStatsProfile());

        config.setStatsInterval(INTERVAL_SEC);
        assertEquals(INTERVAL_SEC, config.getStatsInterval());

        config.setStatsProfile(StatsControl.Profile.REGULAR);
        assertEquals(StatsControl.Profile.REGULAR, config.getStatsProfile());

        config.setStatsPrettyPrint(true);
        assertTrue(config.getStatsPrettyPrint());

        config.setStatsHandler(jsonStats -> {
            if (statsList != null) {
                statsList.add(jsonStats);
            }
        });
        assertNotNull(config.getStatsHandler());
        /* suppress logging noise unless explicitly set to true */
        boolean enableLog = Boolean.getBoolean(
            NoSQLHandleConfig.STATS_ENABLE_LOG_PROPERTY);
        config.setStatsEnableLog(enableLog);
    }

    @Override
    public void afterTest() throws Exception {
        dropTable(TABLE_NAME);
        super.afterTest();
    }

    /**
     * Test we have the correct NoSQLHandle with stats enabled.
     */
    @Test
    public void testStatsControl() {
        StatsControl statsControl = handle.getStatsControl();
        assertEquals(INTERVAL_SEC, statsControl.getInterval());
        assertEquals(StatsControl.Profile.REGULAR, statsControl.getProfile());
        assertTrue(statsControl.getPrettyPrint());
        assertNotNull(statsControl.getStatsHandler());
        assertTrue(statsControl.isStarted());
    }

    /**
     * Test stats handle.
     */
    @Test
    public void testStatsHandle()
        throws InterruptedException {
        // Start fresh
        statsList = new ArrayList<>();

        final int numMajor = 3;
        final int numPerMajor = 2;
        final int recordKB = 2;

        loadRows(numMajor, numPerMajor, recordKB);

        // To get per query stats switch to ALL stats profile
        handle.getStatsControl().setProfile(StatsControl.Profile.ALL);
        assertEquals(StatsControl.Profile.ALL,
            handle.getStatsControl().getProfile());

        String query = "select * from " + TABLE_NAME;
        doQuery(handle, query);

        handle.getStatsControl().setProfile(StatsControl.Profile.REGULAR);
        assertEquals(StatsControl.Profile.REGULAR,
            handle.getStatsControl().getProfile());

        // the code above should have triggered the stats collection.
        // wait for the stats handle to be called at the end of the interval
        Thread.sleep(SLEEP_MSEC);

        assertNotNull(statsList);

        // In the interval, stats handle is most likely to be called 1 time, but
        // it's possible to be called more times.
        assertTrue(statsList.size() > 0);

        // Check basics, all entries should contain the following:
        for (MapValue stats : statsList) {
            assertNotNull(stats.get("clientId"));
            assertNotNull(stats.get("clientId").asString());
            assertTrue(stats.get("clientId").asString().getValue()
                .length() > 0);
            assertNotNull(stats.get("startTime"));
            assertTrue(stats.get("startTime").isTimestamp());
            assertNotNull(stats.get("endTime"));
            assertTrue(stats.get("endTime").isTimestamp());
            assertNotNull(stats.get("requests"));
            assertTrue(stats.get("requests").isArray());
        }

        // Even if there are more entries at least 1 should contain:
        //  - at least 1 request
        long count =
            statsList.stream()
                .filter(s -> s.get("requests") != null &&
                    s.get("requests").isArray() &&
                    s.get("requests").asArray().size() > 0 )
            .count();
        assertTrue(count >=1);

        // At least one entry should have a request with everything inside
        count =
            statsList.stream()
                .filter(s -> s.get("requests") != null &&
                    s.get("requests").isArray() &&
                    s.get("requests").asArray().size() > 0 &&
                    s.get("requests").asArray().get(0).isMap() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestCount") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestCount").isNumeric() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("name") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("name").isString() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("rateLimitDelayMs") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("rateLimitDelayMs").isNumeric() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("errors") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("errors").isNumeric() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").isMap() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap() != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("min") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("min").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("avg") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("avg").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("max") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("requestSize").asMap().get("max").isNumeric() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").isMap() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap() != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("min") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("min").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("avg") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("avg").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("max") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("resultSize").asMap().get("max").isNumeric() &&


                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").isMap() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap() != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("min") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("min").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("avg") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("avg").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("max") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("httpRequestLatencyMs").asMap()
                        .get("max").isNumeric() &&

                    s.get("requests").asArray().get(0).asMap()
                        .get("retry") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").isMap() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap() != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("delayMs") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("delayMs").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("authCount") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("authCount").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("throttleCount") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("throttleCount").isNumeric() &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("count") != null &&
                    s.get("requests").asArray().get(0).asMap()
                        .get("retry").asMap().get("count").isNumeric()
                ).count();
        assertTrue(count >=1);

        //  - at least 1 connection
        count =
            statsList.stream()
                .filter(s -> s.get("connections") != null &&
                    s.get("connections").isMap() &&
                    s.get("connections").asMap().get("min") != null &&
                    s.get("connections").asMap().get("max") != null &&
                    s.get("connections").asMap().get("avg") != null )
                .count();
        assertTrue(count >=1);

        //  - at least 1 query
        count =
            statsList.stream().filter(s -> s.get("queries") != null &&
                s.get("queries").isArray() &&
                s.get("queries").asArray().size()==1 &&
                s.get("queries").asArray().get(0) != null &&
                s.get("queries").asArray().get(0).isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("query") != null &&
                s.get("queries").asArray().get(0).asMap().get("query")
                    .asString().getValue().equals(query))
            .count();
        assertTrue(count >=1);

        //  - at least 1 query with everything in it
        count =
            statsList.stream().filter(s -> s.get("queries") != null &&
                s.get("queries").isArray() &&
                s.get("queries").asArray().size()==1 &&
                s.get("queries").asArray().get(0) != null &&
                s.get("queries").asArray().get(0).isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("query") != null &&

                s.get("queries").asArray().get(0).asMap()
                    .get("doesWrites") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("doesWrites").isBoolean() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("unprepared") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("unprepared").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestCount") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestCount").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("count") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("count").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("simple") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("simple").isBoolean() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("rateLimitDelayMs") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("rateLimitDelayMs").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("errors") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("errors").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("min") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("min").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("avg") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("avg").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("max") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("requestSize").asMap().get("max").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("min") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("min").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("avg") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("avg").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("max") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("resultSize").asMap().get("max").isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("min") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("min")
                    .isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("avg") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("avg")
                    .isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("max") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("max")
                    .isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("95th") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("95th")
                    .isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("99th") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("httpRequestLatencyMs").asMap().get("99th")
                    .isNumeric() &&

                s.get("queries").asArray().get(0).asMap()
                    .get("retry") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").isMap() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("delayMs") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("delayMs").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("authCount") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("authCount").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("throttleCount") != null &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("throttleCount").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("count").isNumeric() &&
                s.get("queries").asArray().get(0).asMap()
                    .get("retry").asMap().get("count").isNumeric()
            ).count();
        assertTrue(count >=1);
    }

    /**
     * Test stop/start.
     */
    @Test
    public void testStopStart()
        throws InterruptedException {

        // stop observations and wait at least 3 sec
        handle.getStatsControl().stop();
        assertFalse(handle.getStatsControl().isStarted());

        // wait for all observations to be reset
        Thread.sleep(SLEEP_MSEC);

        // Start fresh
        statsList = new ArrayList<>();

        final int numMajor = 3;
        final int numPerMajor = 2;
        final int recordKB = 2;

        loadRows(numMajor, numPerMajor, recordKB);

        // To get per query stats switch to ALL stats profile
        handle.getStatsControl().setProfile(StatsControl.Profile.ALL);
        assertEquals(StatsControl.Profile.ALL,
            handle.getStatsControl().getProfile());

        String query = "select * from " + TABLE_NAME;
        doQuery(handle, query);

        handle.getStatsControl().setProfile(StatsControl.Profile.REGULAR);
        assertEquals(StatsControl.Profile.REGULAR,
            handle.getStatsControl().getProfile());

        // the code above should have triggered the stats collection.
        // wait for the stats handle to be called at the end of the interval
        Thread.sleep(SLEEP_MSEC);

        assertNotNull(statsList);

        // In the interval it's most likely to be called 1 time, but
        // it's possible to be called more times.
        assertTrue(statsList.size() > 0);

        // All entries should not have any requests:
        long count =
            statsList.stream()
                .filter(s -> s.get("requests") != null &&
                    s.get("requests").isArray() &&
                    s.get("requests").asArray().size() == 0 )
                .count();
        assertEquals(count, statsList.size());

        //  - at no connections
        count =
            statsList.stream()
                .filter(s -> s.get("connections") != null)
                .count();
        assertEquals(count, 0);

        //  - All entries should not have any queries
        count =
            statsList.stream().filter(s -> s.get("queries") != null )
                .count();
        assertEquals(0, count);



        // Start observations and check if stats list contain some
        handle.getStatsControl().start();
        assertTrue(handle.getStatsControl().isStarted());

        // Start fresh
        statsList.clear();

        loadRows(numMajor, numPerMajor, recordKB);

        handle.getStatsControl().setProfile(StatsControl.Profile.ALL);
        assertEquals(StatsControl.Profile.ALL,
            handle.getStatsControl().getProfile());

        doQuery(handle, query);

        handle.getStatsControl().setProfile(StatsControl.Profile.REGULAR);
        assertEquals(StatsControl.Profile.REGULAR,
            handle.getStatsControl().getProfile());

        // the code above should have triggered the stats collection.
        // wait for the stats handle to be called at the end of the interval
        Thread.sleep(SLEEP_MSEC);

        assertNotNull(statsList);

        // In the interval it's most likely to be called 1 time, but
        // it's possible to be called more times.
        assertTrue(statsList.size() > 0);

        // All entries should have many requests:
        count =
            statsList.stream()
                .filter(s -> s.get("requests") != null &&
                    s.get("requests").isArray() &&
                    s.get("requests").asArray().size() > 0 )
                .count();
        assertTrue(count > 0);

        //  - and some connections
        count =
            statsList.stream()
                .filter(s -> s.get("connections") != null)
                .count();
        assertTrue(count > 0);

        //  - and have 1 query
        count =
            statsList.stream().filter(s -> s.get("queries") != null )
                .count();
        assertTrue(count > 0);

        count =
            statsList.stream().filter(s -> s.get("queries") != null &&
                s.get("queries").isArray() &&
                s.get("queries").asArray().size()==1 &&
                s.get("queries").asArray().get(0) != null &&
                s.get("queries").asArray().get(0).isMap() &&
                s.get("queries").asArray().get(0).asMap().get("query") != null
                && s.get("queries").asArray().get(0).asMap().get("query")
                .asString().getValue().equals(query))
                .count();
        assertEquals(1, count);
    }



    /**
     * Loads a number of rows into the TABLE_NAME table
     */
    private void loadRows(int numMajor, int numPerMajor, int nKB) {

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(TABLE_NAME);

        /* Load rows */
        final String longString = genString((nKB - 1) * 1024);
        for (int i = 0; i < numMajor; i++) {
            value.put("sid", i);
            for (int j = 0; j < numPerMajor; j++) {
                value.put("id", j);
                value.put("name", "name_" + i + "_" + j);
                value.put("longString", longString);
                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());
            }
        }
    }
}
