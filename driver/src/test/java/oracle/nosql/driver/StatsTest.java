/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
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

    final static private String TABLE_NAME = "statsTestTable";

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
        assertEquals(600, config.getStatsInterval());
        assertFalse(config.getStatsPrettyPrint());
        assertEquals(StatsControl.Profile.NONE, config.getStatsProfile());

        config.setStatsInterval(3);
        assertEquals(3, config.getStatsInterval());

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
        assertEquals(3, statsControl.getInterval());
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
        // wait for the stats handle to be called at the end of the 3s interval
        Thread.sleep(3200);

        assertNotNull(statsList);

        // In at least 3.1 seconds it's most likely to be called 1 time, but
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
                s.get("queries").asArray().get(0).asMap().get("query") != null
                && s.get("queries").asArray().get(0).asMap().get("query")
                    .asString().getValue().equals(query))
            .count();
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
        Thread.sleep(3200);

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
        // wait for the stats handle to be called at the end of the 3s interval
        Thread.sleep(3200);

        assertNotNull(statsList);

        // In at least 3.2 seconds it's most likely to be called 1 time, but
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
        // wait for the stats handle to be called at the end of the 3s interval
        Thread.sleep(3200);

        assertNotNull(statsList);

        // In at least 3.1 seconds it's most likely to be called 1 time, but
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
