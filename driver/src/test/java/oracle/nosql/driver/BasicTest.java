/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.BinaryProtocol.V4;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableLimits.CapacityMode;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;

import org.junit.Test;

public class BasicTest extends ProxyTestBase {

    @Test
    public void smokeTest() {

        try {
            MapValue key = new MapValue().put("id", 10);

            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            /* drop a table */
            TableResult tres = tableOperation(handle,
                                              "drop table if exists testusers",
                                              null);
            assertNotNull(tres.getTableName());
            assertNull(tres.getTableLimits());

            /* drop again without if exists -- should throw */
            try {
                tres = tableOperation(handle,
                                      "drop table testusers",
                                      null);
                fail("operation should have thrown");
            } catch (TableNotFoundException tnfe) {
                /* success */
            }


            /* Create a table */
            tres = tableOperation(
                handle,
                "create table if not exists testusers(id integer, " +
                "name string, primary key(id))",
                new TableLimits(500, 500, 50));
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Create an index */
            tres = tableOperation(
                handle,
                "create index if not exists Name on testusers(name)",
                null);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* GetTableRequest for table that doesn't exist */
            try {
                GetTableRequest getTable =
                    new GetTableRequest()
                    .setTableName("not_a_table");
                tres = handle.getTable(getTable);
                fail("Table should not be found");
            } catch (TableNotFoundException tnfe) {}

            /* list tables */
            ListTablesRequest listTables =
                new ListTablesRequest();
            ListTablesResult lres = handle.listTables(listTables);
            /*
             * the test cases don't yet clean up so there may be additional
             * tables present, be flexible in this assertion.
             */
            assertTrue(lres.getTables().length >= 1);
            assertNotNull(lres.toString());

            /* getTableUsage. It won't return much in test mode */
            if (!onprem) {
                TableUsageRequest gtu = new TableUsageRequest()
                    .setTableName("testusers").setLimit(2)
                    .setEndTime(System.currentTimeMillis());
                TableUsageResult gtuRes = handle.getTableUsage(gtu);
                assertNotNull(gtuRes);
                assertNotNull(gtuRes.getUsageRecords());
            }

            /* PUT */
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName("testusers");

            PutResult res = handle.put(putRequest);
            assertNotNull(res.getVersion());
            assertWriteKB(res);
            /* put a few more. set TTL to test that path */
            putRequest.setTTL(TimeToLive.ofHours(2));
            for (int i = 20; i < 30; i++) {
                value.put("id", i);
                handle.put(putRequest);
            }

            /*
             * Test ReturnRow for simple put of a row that exists. 2 cases:
             * 1. unconditional (no return info)
             * 2. if absent (will return info)
             */
            value.put("id", 20);
            putRequest.setReturnRow(true);
            PutResult pr = handle.put(putRequest);
            assertNotNull(pr.getVersion()); /* success */
            /* If proxy serial version <= V4,put success does not return row */
            if (proxySerialVersion <= V4) {
                assertNull(pr.getExistingVersion());
                assertNull(pr.getExistingValue());
                assertEquals(0, pr.getExistingModificationTime());
                assertWriteKB(pr);
            } else {
                assertNotNull(pr.getExistingVersion());
                assertNotNull(pr.getExistingValue());
                assertTrue(pr.getExistingModificationTime() != 0);
                assertReadKB(pr);
                assertWriteKB(pr);
            }

            putRequest.setOption(Option.IfAbsent);
            pr = handle.put(putRequest);
            assertNull(pr.getVersion()); /* failure */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertReadKB(pr);

            /* clean up */
            putRequest.setReturnRow(false);
            putRequest.setOption(null);

            /* GET */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName("testusers");

            GetResult res1 = handle.get(getRequest);
            assertNotNull(res1.getJsonValue());

            /* DELETE */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName("testusers");

            DeleteResult del = handle.delete(delRequest);
            assertTrue(del.getSuccess());
            assertWriteKB(del);

            /* GET -- no row, it was removed above */
            getRequest.setTableName("testusers");
            res1 = handle.get(getRequest);
            assertNull(res1.getValue());

            /* GET -- no table */
            try {
                getRequest.setTableName("not_a_table");
                res1 = handle.get(getRequest);
                fail("Attempt to access missing table should have thrown");
            } catch (TableNotFoundException nse) {
                /* success */
            }

            /* PUT -- invalid row -- this will throw */
            try {
                value.remove("id");
                value.put("not_a_field", 1);
                res = handle.put(putRequest);
                fail("Attempt to put invalid row should have thrown");
            } catch (IllegalArgumentException iae) {
                /* success */
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        }
    }

    @Test
    public void testSimpleThroughput() throws Exception {

        assumeTrue(onprem == false);

        final String create = "create table testusersTp(id integer," +
            "name string, primary key(id))";

        /* Create a table */
        tableOperation(handle, create, new TableLimits(500, 500, 50));

        MapValue value = new MapValue().put("id", 10).put("name", "jane");

        /*
         * Handle some Put cases
         */
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("testusersTp");


        PutResult res = handle.put(putRequest);
        assertNotNull("Put failed", res.getVersion());
        int origRead = res.getReadKB();
        int origWrite = res.getWriteKB();
        assertEquals(1, origWrite);
        assertEquals(0, origRead);
        assertNull("Not expecting previous version", res.getExistingVersion());
        assertNull("Not expecting previous value", res.getExistingValue());
        assertEquals(0, res.getExistingModificationTime());


        /*
         * do a second put. Read should still be 0, write will increase
         * because it's an update, which counts the "delete"
         */
        res = handle.put(putRequest);
        int newRead = res.getReadKB();
        int newWrite = res.getWriteKB();
        assertEquals(2*origWrite, newWrite);
        assertEquals(0, newRead);
        assertNull("Not expecting previous version", res.getExistingVersion());
        assertNull("Not expecting previous value", res.getExistingValue());
        assertEquals(0, res.getExistingModificationTime());

        /* set return row and check */
        putRequest.setReturnRow(true);
        res = handle.put(putRequest);
        newRead = res.getReadKB();
        newWrite = res.getWriteKB();
        assertEquals(2*origWrite, newWrite);
        /* If proxy serial version <= V4,put success does not return row */
        if (proxySerialVersion <= V4) {
            assertEquals(0, newRead);
            assertNull("Not expecting previous version", res.getExistingVersion());
            assertNull("Not expecting previous value", res.getExistingValue());
            assertEquals(0, res.getExistingModificationTime());
        } else {
            assertEquals(1, newRead);
            assertNotNull("Expecting previous version",
                res.getExistingVersion());
            assertNotNull("Expecting previous value", res.getExistingValue());
            assertTrue(res.getExistingModificationTime() != 0);
        }

        /* make it ifAbsent and verify read and write consumption */
        putRequest.setOption(PutRequest.Option.IfAbsent);
        res = handle.put(putRequest);
        assertNull("Put should have failed", res.getVersion());
        /* use read units because in a write, readKB != readUnits */
        newRead = res.getReadUnits();
        newWrite = res.getWriteKB();
        /*
         * no write, but read is min read + record size, former for the version
         * and the latter for the value
         */
        assertEquals(0, newWrite);
        assertEquals(1 + origWrite, newRead);
        assertNotNull("Expecting previous version",
                res.getExistingVersion());
        assertNotNull("Expecting previous value", res.getExistingValue());
        assertTrue(res.getExistingModificationTime() != 0);
    }

    /**
     * Test bad urls.
     */
    @Test
    public void testBadURL() throws Exception {
        if (secure) {
            return; /* these will fail in a secure environment */
        }
        /* bad port */
        tryURL(new URL("http", getServiceHost(), getServicePort() + 7, "/"));
        /* bad host */
        tryURL(new URL("http", "nohost", getServicePort(), "/"));
    }

    private void tryURL(URL url) {
        NoSQLHandle myhandle = null;
        try {
            NoSQLHandleConfig config = new NoSQLHandleConfig(url);
            configAuth(config);
            try {
                myhandle = getHandle(config);
                PutRequest putRequest = new PutRequest()
                    .setValue(new MapValue().put("id", 1))
                    .setTableName("testusers");

                myhandle.put(putRequest);
                fail("Operation should have failed");
            } catch (Exception e) {
                /* success */
            }
        } catch (Exception e) {
            fail("?: " + e);
        } finally {
            if (myhandle != null) {
                myhandle.close();
            }
        }
    }

    @Test
    public void testListTables() {
        final int numTables = 5;
        final String ddlFmt =
            "create table %s (id integer, name string, primary key(id))";
        final TableLimits tableLimits = new TableLimits(10, 10, 1);
        final String[] namePrefix = new String[] {"USERB", "userA", "userC"};

        /*
         * create tables
         */
        TableResult tres;
        Set<String> nameSorted = new TreeSet<>();
        for (int i = 0; i < numTables; i++) {
            String tableName = namePrefix[i % namePrefix.length] + i;
            tres = tableOperation(handle,
                                  String.format(ddlFmt, tableName),
                                  tableLimits);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());
            nameSorted.add(tableName);
        }

        /*
         * List all tables
         */
        ListTablesRequest req = new ListTablesRequest();
        ListTablesResult res = handle.listTables(req);
        assertTrue(res.getTables().length > 0);

        /*
         * List all tables with limit
         */
        int[] values = new int[] {0, 6, 2, 1};
        for (int limit : values) {
            doListTables(limit);
        }
    }

    /* Run list tables with limit specified */
    private List<String> doListTables(int limit) {
        List<String> tables = new ArrayList<>();

        ListTablesRequest req = new ListTablesRequest();
        req.setLimit(limit);
        ListTablesResult res;
        while(true) {
            res = handle.listTables(req);
            if (res.getTables().length > 0) {
                tables.addAll(Arrays.asList(res.getTables()));
            }

            if (limit == 0 || res.getTables().length < limit) {
                break;
            }
            assertEquals(limit, res.getTables().length);
            req.setStartIndex(res.getLastReturnedIndex());
        }
        return tables;
    }

    /**
     * Tests serialization of types, including some coercion to schema
     * types in the proxy.
     */
    @Test
    public void typeTest() throws Exception {

        final String TABLE_CREATE =
            "create table if not exists Types( " +
            "id integer, " +
            "primary key(id), " +
            "longField long, " +
            "doubleField double, " +
            "stringField string, " +
            "numberField number, " +
            "enumField enum(a,b,c)" +
            ")";

        final String jsonString =
            "{" +
            "\"id\":1, " +
            "\"longField\": 123 ," + // int => long
            "\"doubleField\":4 ," +  // int => double
            "\"stringField\":\"abc\" ," + // no coercion
            "\"numberField\":4.5 ," + // double => number
            "\"enumField\":\"b\"" + // string => enum
            "}";
        TableResult tres;

        tres = tableOperation(handle,
                              TABLE_CREATE,
                              new TableLimits(50, 50, 50));

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PutRequest pr = new PutRequest().setValueFromJson(jsonString, null).
            setTableName("Types");
        PutResult pres = handle.put(pr);
        assertNotNull(pres.getVersion());
    }

    @Test
    public void recreateTest() throws Exception {
        final String CREATE_TABLE =
            "create table recreate( " +
            "id integer, " +
            "primary key(id), " +
            "name string)";
        final String DROP_TABLE = "drop table recreate";
        TableResult tres = tableOperation(handle,
                                          CREATE_TABLE,
                                          new TableLimits(50, 50, 50));

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PutRequest pr = new PutRequest()
            .setTableName("recreate")
            .setValue(new MapValue().put("id", 1).put("name", "joe"));
        PutResult pres = handle.put(pr);
        assertNotNull(pres.getVersion());

        try {
            tres = tableOperation(handle, DROP_TABLE, null);
        } catch (TableNotFoundException e) {
            /* versions before 20.3 had known issues with drop table */
            if (checkKVVersion(20, 3, 1)) {
                throw e;
            }
        }

        tres = tableOperation(handle,
                              CREATE_TABLE,
                              new TableLimits(50, 50, 50));
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        pres = handle.put(pr);
        assertNotNull(pres.getVersion());
    }


    /**
     * This test does a lot of simple operations in a loop in multiple threads,
     * looking for HTTP transport problems. This is probably temporary.
     */
    @Test
    public void httpTest() {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        for (int i = 0; i < 6; i++) {
            tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        doHttpTest();
                        return null;
                    }
                });
        }
        try {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for(Future<Void> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        }
    }

    private void doHttpTest() {
        try {

            MapValue key = new MapValue().put("id", 10);
            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            for (int i = 0; i < 10; i++) {
                try {
                    /* Create a table */
                    TableResult tres = tableOperation(
                        handle,
                        "create table if not exists testusers(id integer, " +
                        "name string, primary key(id))",
                        new TableLimits(500, 500, 50));
                    assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                } catch (Exception e) {
                    System.out.println("httpTest: exception in Thread " +
                                       Thread.currentThread().getId() +
                                       " on attempt " + i + ": " +
                                       e);
                }
            }

            for (int i = 0; i < 100; i++) {
                /* PUT */
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName("testusers")
                    .setTimeout(10000);

                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());
                assertWriteKB(res);

                /* GET */
                GetRequest getRequest = new GetRequest()
                    .setKey(key)
                    .setTableName("testusers")
                    .setTimeout(10000);

                GetResult res1 = handle.get(getRequest);
                assertNotNull("Get failed", res1.getJsonValue());
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        }
    }

    @Test
    public void testPutGetDelete() {

        final String tableName = "testusers";
        final int recordKB = 2;

        final short serialVersion =
              ((NoSQLHandleImpl)handle).getSerialVersion();

        final String stmt = "create table if not exists testusers(id " +
                            "integer, name string, primary key(id))";
        /* Create a table */
        try {
            TableResult tres = tableOperation(handle, stmt,
                new TableLimits(0, 0, 50, CapacityMode.ON_DEMAND));
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        } catch (IllegalArgumentException iae) {
            /* expected in V2 */
            if (serialVersion > 2) {
                throw iae;
            }
            TableResult tres = tableOperation(handle, stmt,
                new TableLimits(500, 500, 50, CapacityMode.PROVISIONED));
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());
        }

        final String name = genString((recordKB - 1) * 1024);
        MapValue value = new MapValue().put("id", 10).put("name", name);
        MapValue newValue = new MapValue().put("id", 11).put("name", name);
        MapValue newValue1 = new MapValue().put("id", 12).put("name", name);
        MapValue newValue2 = new MapValue().put("id", 13).put("name", name);


        /* Put a row */
        PutRequest putReq = new PutRequest()
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        PutResult putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* Put a row again with SetReturnRow(false).
         * expect no row returned
         */
        putReq.setReturnRow(false);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        Version oldVersion = putRes.getVersion();


        /*
         * Put row again with SetReturnRow(true),
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        /* If proxy serial version <= V4,put success does not return row */
        if (proxySerialVersion <= V4) {
            /* expect no existing row returned */
            checkPutResult(putReq, putRes,
                           true /* shouldSucceed */,
                           false /* rowPresent */,
                           null /* expPrevValue */,
                           null /* expPrevVersion */,
                           false, /* modtime should be zero */
                           recordKB);
        } else {
            /* expect existing row returned */
            checkPutResult(putReq, putRes,
                           true /* shouldSucceed */,
                           true /* rowPresent */,
                           value /* expPrevValue */,
                           oldVersion /* expPrevVersion */,
                           true, /* modtime should be zero */
                           recordKB);
        }
        oldVersion = putRes.getVersion();

        /*
         * Put a new row with SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq = new PutRequest()
            .setValue(newValue)
            .setTableName(tableName)
            .setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* PutIfAbsent an existing row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false  /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        /*
         * PutIfAbsent fails + SetReturnRow(true),
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       true  /* rowPresent */,
                       value /* expPrevValue */,
                       oldVersion /* expPrevVersion */,
                       (serialVersion > 2), /* modtime should be recent */
                       recordKB);

        /* PutIfPresent an existing row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        oldVersion = putRes.getVersion();

        /*
         * PutIfPresent succeed + SetReturnRow(true),
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        /* If proxy serial version <= V4,put success does not return row */
        if (proxySerialVersion <= V4) {
            checkPutResult(putReq, putRes,
                           true /* shouldSucceed */,
                           false /* rowPresent */,
                           null /* expPrevValue */,
                           null /* expPrevVersion */,
                           false, /* modtime should be zero */
                           recordKB);
        } else {
            checkPutResult(putReq, putRes,
                           true /* shouldSucceed */,
                           true /* rowPresent */,
                           value /* expPrevValue */,
                           oldVersion /* expPrevVersion */,
                           true, /* modtime should be zero */
                           recordKB);
        }
        Version ifVersion = putRes.getVersion();

        /* PutIfPresent an new row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(newValue1)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        /*
         * PutIfPresent fail + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* PutIfAbsent an new row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(newValue1)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* PutIfAbsent success + SetReturnRow(true) */
        putReq.setValue(newValue2).setReturnRow(true);
        putRes =  handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true  /* shouldSucceed */,
                       false /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /*
         * PutIfVersion an existing row with unmatched version, it should fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(oldVersion)
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       false  /* rowPresent */,
                       null  /* expPrevValue */,
                       null  /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        /*
         * PutIfVersion fails + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       false /* shouldSucceed */,
                       true  /* rowPresent */,
                       value /* expPrevValue */,
                       ifVersion /* expPrevVersion */,
                       (serialVersion > 2), /* modtime should be recent */
                       recordKB);

        /*
         * Put an existing row with matching version, it should succeed.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(ifVersion)
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        ifVersion = putRes.getVersion();
        /*
         * PutIfVersion succeed + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setMatchVersion(ifVersion).setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
                       true /* shouldSucceed */,
                       false /* rowPresent */,
                       null /* expPrevValue */,
                       null /* expPrevVersion */,
                       false, /* modtime should be zero */
                       recordKB);
        Version newVersion = putRes.getVersion();

        /*
         * Put with IfVersion but no matched version is specified, put should
         * fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setValue(value)
            .setDurability(Durability.COMMIT_SYNC)
            .setTableName(tableName);
        try {
            putRes = handle.put(putReq);
            fail("Put with IfVersion should fail");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Get
         */

        /* Get a row */
        MapValue key = new MapValue().put("id", 10);
        GetRequest getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       true /* rowPresent*/,
                       value,
                       null, /* Don't check version if Consistency.EVENTUAL */
                       (serialVersion > 2), /* modtime should be recent */
                       recordKB);

        /*
         * get the row version of the same row using a query and check that
         * the version can be used in a conditional put/delete.
         * Note we can't rely on comparing the byte arrays for each, because
         * the arrays may be slightly different based on versions of client
         * and server in use.
         */
        try (QueryRequest queryReq = new QueryRequest()) {
            final String versionQuery = "select row_version($t) as version " +
                "from " + tableName + " $t where id = 10";
            queryReq.setStatement(versionQuery);
            QueryResult queryRet = handle.query(queryReq);
            MapValue result = queryRet.getResults().get(0);
            Version qVersion = Version.createVersion(
                result.get("version").asBinary().getValue());

            /*
             * Put an existing row with matching version, it should succeed.
             */
            putReq = new PutRequest()
                .setOption(Option.IfVersion)
                .setMatchVersion(qVersion)
                .setValue(value)
                .setDurability(Durability.COMMIT_SYNC)
                .setTableName(tableName);
            putRes = handle.put(putReq);
            checkPutResult(putReq, putRes,
                           true /* shouldSucceed */,
                           false /* rowPresent */,
                           null /* expPrevValue */,
                           null /* expPrevVersion */,
                           false, /* modtime should be zero */
                           recordKB);
            newVersion = putRes.getVersion();
        }

         /*
          * Get the version from a query again, and his time do a
          * conditional delete
          */
        try (QueryRequest queryReq = new QueryRequest()) {
            final String versionQuery = "select row_version($t) as version " +
                "from " + tableName + " $t where id = 10";
            queryReq.setStatement(versionQuery);
            QueryResult queryRet = handle.query(queryReq);
            MapValue result = queryRet.getResults().get(0);
            Version qVersion = Version.createVersion(
                result.get("version").asBinary().getValue());

            key = new MapValue().put("id", 10);
            DeleteRequest delReq = new DeleteRequest()
                .setMatchVersion(qVersion)
                .setKey(key)
                .setTableName(tableName);
            DeleteResult delRes = handle.delete(delReq);
            checkDeleteResult(delReq, delRes,
                              true  /* shouldSucceed */,
                              false  /* rowPresent */,
                              null  /* expPrevValue */,
                              null  /* expPrevVersion */,
                              false, /* modtime should be zero */
                              recordKB);
        }

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        newVersion = putRes.getVersion();

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       true /* rowPresent*/,
                       value,
                       newVersion,
                       (serialVersion > 2), /* modtime should be recent */
                       recordKB);

        /* Get non-existing row */
        key = new MapValue().put("id", 100);
        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       false /* rowPresent*/,
                       null  /* expValue */,
                       null  /* expVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
                       false /* rowPresent*/,
                       null  /* expValue */,
                       null  /* expVersion */,
                       false, /* modtime should be zero */
                       recordKB);

        /* Delete a row */
        key = new MapValue().put("id", 10);
        DeleteRequest delReq = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        oldVersion = putRes.getVersion();
        assertNotNull(oldVersion);

        /* Delete succeed + setReturnRow(true) */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        /* If proxy serial version <= V4,put success does not return row */
        if (proxySerialVersion <= V4) {
            checkDeleteResult(delReq, delRes,
                              true /* shouldSucceed */,
                              false /* rowPresent */,
                              null /* expPrevValue */,
                              null /* expPrevVersion */,
                              false, /* modtime should be zero */
                              recordKB);
        } else {
            checkDeleteResult(delReq, delRes,
                              true /* shouldSucceed */,
                              true /* rowPresent */,
                              value /* expPrevValue */,
                              oldVersion /* expPrevVersion */,
                              true, /* modtime should be zero */
                              recordKB);
        }

        /* Delete fail + setReturnRow(true), no existing row returned. */
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();

        /* DeleteIfVersion with unmatched version, it should fail */
        delReq = new DeleteRequest()
            .setMatchVersion(oldVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /*
         * DeleteIfVersion with unmatched version + setReturnRow(true),
         * the existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          true  /* rowPresent */,
                          value /* expPrevValue */,
                          ifVersion /* expPrevVersion */,
                          (serialVersion > 2), /* modtime should be recent */
                          recordKB);

        /* DeleteIfVersion with matched version, it should succeed. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* rowPresent */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* Put the row back to store */
        putReq = new PutRequest().setValue(value).setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();

        /*
         * DeleteIfVersion with matched version + setReturnRow(true),
         * it should succeed but no existing row returned.
         */
        delReq.setMatchVersion(ifVersion).setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          true  /* shouldSucceed */,
                          false  /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);

        /* DeleteIfVersion with a key not existed, it should fail. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);
        /*
         * DeleteIfVersion with a key not existed + setReturnRow(true),
         * it should fail and no existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
                          false /* shouldSucceed */,
                          false /* returnRow */,
                          null  /* expPrevValue */,
                          null  /* expPrevVersion */,
                          false, /* modtime should be zero */
                          recordKB);
    }

    /*
     * Test GetIndexesRequest.
     */
    @Test
    public void testGetIndexes() {

        /* Request to get all indexes */
        final GetIndexesRequest getAllIndexes = new GetIndexesRequest()
            .setTableName("testusers");

        /* Request to get index idxName */
        final GetIndexesRequest getIndexName = new GetIndexesRequest()
            .setTableName("testusers")
            .setIndexName("idxName");

        GetIndexesResult giRes;

        /* Table does not exist, expects to get TableNotFoundException */
        try {
            giRes = handle.getIndexes(getAllIndexes);
            fail("Expected to catch TableNotFoundException");
        } catch (TableNotFoundException tnfe) {
            /* Succeed */
        }

        /* Create table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists testusers(id integer, " +
            "name string, age integer, primary key(id))",
            new TableLimits(500, 500, 50));
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Get indexes, 0 index returned */
        giRes = handle.getIndexes(getAllIndexes);
        assertTrue(giRes.getIndexes().length == 0);

        /* Get index idxName, expects to get IndexNotFoundException */
        try {
            giRes = handle.getIndexes(getIndexName);
            fail("Expected to caught IndexNotFoundException but not");
        } catch (IndexNotFoundException infe) {
            /* Succeed */
        }

        /* Create indexes */
        tres = tableOperation(
            handle,
            "create index if not exists idxName on testusers(name)",
            null);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        tres = tableOperation(
            handle,
            "create index if not exists idxAgeName on testusers(age, name)",
            null);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Get indexes, 2 indexes returned */
        giRes = handle.getIndexes(getAllIndexes);
        assertTrue(giRes.getIndexes().length == 2);

        /* Get idxName, 1 index returned */
        giRes = handle.getIndexes(getIndexName);
        assertTrue(giRes.getIndexes().length == 1);

        /* Invalid argument - miss table name */
        try {
            GetIndexesRequest badReq = new GetIndexesRequest();
            handle.getIndexes(badReq);
            fail("Expected IllegalArgumentException " +
                 "because of missing table name");
        } catch (IllegalArgumentException iae) {
            /* Succeed */
        }
    }

    /*
     * Test on put values(compatible or incompatible) to KV table non-numeric
     * primitive data types:
     *  o BOOLEAN
     *  o STRING
     *  o ENUM
     *  o TIMESTAMP
     *  o BINARY
     *  o FIXED_BINARY
     *  o JSON
     */
    @Test
    public void testNonNumericDataTypes() {
        final String tableName = "DataTypes";
        final String createTableDdl =
            "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                "id INTEGER, " +
                "bl BOOLEAN, " +
                "s STRING, " +
                "e ENUM(red, yellow, blue), " +
                "ts TIMESTAMP(9), " +
                "bi BINARY, " +
                "fbi BINARY(10), " +
                "json JSON," +
                "PRIMARY KEY(id)" +
            ")";

        final FieldValue intVal = new IntegerValue(1);
        final FieldValue boolVal = BooleanValue.trueInstance();
        final FieldValue strVal = new StringValue("oracle nosql");
        final FieldValue enumStrVal = new StringValue("red");

        final Timestamp ts = Timestamp.valueOf("2018-05-02 10:23:42.123");
        final FieldValue tsVal = new TimestampValue(ts);
        final FieldValue tsStrVal = new StringValue("2018-05-02T10:23:42.123");

        byte[] byte10 = genBytes(10);
        byte[] byte20 = genBytes(20);
        final FieldValue bi10Val = new BinaryValue(byte10);
        final FieldValue bi20Val = new BinaryValue(byte20);
        final FieldValue strByte10 =
            new StringValue(BinaryValue.encodeBase64(byte10));
        final FieldValue strByte20 =
            new StringValue(BinaryValue.encodeBase64(byte20));

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            createTableDdl,
            new TableLimits(500, 500, 50));
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        FieldValue[] invalidValues;
        FieldValue[] validValues;
        String targetField;

        /* Boolean type */
        targetField = "bl";
        invalidValues = new FieldValue[] {intVal, tsVal, bi10Val};
        validValues = new FieldValue[] {boolVal, strVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* String type */
        targetField = "s";
        invalidValues = new FieldValue[] {intVal, boolVal, tsVal, bi10Val};
        validValues = new FieldValue[] {strVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Emum type */
        targetField = "e";
        invalidValues =
            new FieldValue[] {intVal, boolVal, strVal, tsVal, bi10Val};
        validValues = new FieldValue[] {enumStrVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Timestamp type */
        targetField = "ts";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, bi10Val};
        validValues = new FieldValue[] {tsVal, tsStrVal};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Binary type */
        targetField = "bi";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal};
        validValues = new FieldValue[] {bi10Val, bi20Val, strByte10, strByte20};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* Fixed binary type */
        targetField = "fbi";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal,
                                          bi20Val, strByte20};
        validValues = new FieldValue[] {bi10Val, strByte10};
        runPut(tableName, targetField, invalidValues, false);
        runPut(tableName, targetField, validValues, true);

        /* JSON type */
        targetField = "json";
        invalidValues = new FieldValue[] {intVal, boolVal, strVal, tsVal,
                                          bi10Val};
        runPut(tableName, targetField, validValues, true);
    }

    @Test
    public void testNullJsonNull() {
        final String createTable1 =
            "create table tjson(id integer, info json, primary key(id))";
        final String createTable2 =
            "create table trecord(id integer, " +
                                  "info record(name string, age integer), " +
                                  "primary key(id))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1));
        tableOperation(handle, createTable2, new TableLimits(10, 10, 1));

        MapValue rowNull = new MapValue()
            .put("id", 0)
            .put("info",
                 new MapValue()
                     .put("name", NullValue.getInstance())
                     .put("age", 20));
        MapValue rowJsonNull = new MapValue()
            .put("id", 0)
            .put("info",
                 new MapValue()
                     .put("name", JsonNullValue.getInstance())
                     .put("age", 20));

        MapValue[] rows = new MapValue[] {rowNull, rowJsonNull};
        Map<String, MapValue> tableExpRows = new HashMap<String, MapValue>();
        tableExpRows.put("tjson", rowJsonNull);
        tableExpRows.put("trecord", rowNull);

        /*
         * Put rows with NullValue or JsonNullValue, they should be converted
         * to the right value for the target type.
         */
        for (Map.Entry<String, MapValue> e : tableExpRows.entrySet()) {
            String table = e.getKey();
            MapValue expRow = e.getValue();

            for (MapValue row : rows) {
                PutRequest putReq = new PutRequest()
                    .setTableName(table)
                    .setValue(row);
                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);

                MapValue key = new MapValue().put("id", row.get("id"));
                GetRequest getReq = new GetRequest()
                    .setTableName(table)
                    .setConsistency(Consistency.ABSOLUTE)
                    .setKey(key);
                GetResult getRet = handle.get(getReq);
                assertEquals(expRow, getRet.getValue());
                assertNotNull(getRet.getVersion());
                assertTrue(Arrays.equals(pVersion.getBytes(),
                                         getRet.getVersion().getBytes()));
            }
        }

        /*
         * Query with variable for json field and set NullValue or
         * JsonNullValue to variable, the NullValue is expected to be converted
         * to JsonNullValue.
         */
        String query = "declare $name json;" +
            "select * from tjson t where t.info.name = $name";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        prepStmt.setVariable("$name", JsonNullValue.getInstance());
        try (QueryRequest queryReq = new QueryRequest()) {
            queryReq.setPreparedStatement(prepStmt);
            QueryResult queryRet = handle.query(queryReq);
            assertEquals(1, queryRet.getResults().size());
            assertEquals(rowJsonNull, queryRet.getResults().get(0));

            prepStmt.setVariable("$name", NullValue.getInstance());
            queryRet = handle.query(queryReq);
            assertEquals(0, queryRet.getResults().size());
        }
    }

    @Test
    public void testExactMatch() {
        final String tableName = "tMatch";
        final String createTable =
            "create table tMatch(id integer, name string, " +
            "age integer, primary key(id))";

        tableOperation(handle, createTable, new TableLimits(10, 10, 1));

        /* use extra values, not exact match */
        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", "myname")
            .put("age", 5)
            .put("extra", "foo");

        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(value);
        PutResult putRet = handle.put(putReq);
        assertNotNull(putRet.getVersion());

        /* set exact match to true, this shoudl fail */
        putReq.setExactMatch(true);
        try {
            putRet = handle.put(putReq);
            fail("Put should have thrown IAE");
        } catch (Exception e) {
            /* success */
        }

        /* test via query insert */
        String insertQ =
            "insert into tMatch(id, name, age) values(5, 'fred', 6)";
        try (QueryRequest qReq = new QueryRequest()) {
            qReq.setStatement(insertQ);
            QueryResult qRes = handle.query(qReq);
            for (MapValue res : qRes.getResults()) {
                assertEquals(1, res.get("NumRowsInserted").getInt());
            }
        }

        /* try using prepared query */
        insertQ =
            "insert into tMatch(id, name, age) values(6, 'jack', 6)";
        PrepareRequest prepReq = new PrepareRequest().setStatement(insertQ);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();
        try (QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(prepStmt);
            QueryResult qRes = handle.query(qReq);
            for (MapValue res : qRes.getResults()) {
                assertEquals(1, res.get("NumRowsInserted").getInt());
            }
        }
    }

    @Test
    public void testIdentityColumn() {
        final String tableName = "tIdentity";
        final String createTable1 =
            "create table tIdentity(id integer, id1 long generated always " +
            "as identity, name string, primary key(shard(id), id1))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1));

        MapValue value = new MapValue()
            .put("id", 1)
            .put("name", "myname");

        /* test single put */
        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(value)
            .setIdentityCacheSize(5);
        PutResult putRet = handle.put(putReq);
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());

        /* test WriteMultiple */
        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for (int i = 0; i < 10; i++) {
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setIdentityCacheSize(i)
                .setTableName(tableName);
            /* cause last operation to fail and not return a generated value */
            if (i == 9) {
                putRequest.setOption(PutRequest.Option.IfPresent);
            }
            wmReq.add(putRequest, false);
        }

        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());
        int i = 0;
        int lastIdVal = -1;
        for (OperationResult result : wmRes.getResults()) {
            if (i++ == 9) {
                assertNull(result.getGeneratedValue());
            } else {
                assertNotNull(result.getGeneratedValue());
                if (lastIdVal < 0) {
                    lastIdVal = result.getGeneratedValue().getInt();
                } else {
                    assertEquals(lastIdVal + 1,
                                 result.getGeneratedValue().getInt());
                    lastIdVal = result.getGeneratedValue().getInt();
                }
            }
        }

        /*
         * Verify that a failed operation (without an exception) will not
         * return a generated value. The system may have generated one, but
         * it is not relevant in this case.
         */
        putReq.setOption(PutRequest.Option.IfPresent);
        putRet = handle.put(putReq);
        assertNull(putRet.getGeneratedValue());


        /* try an invalid case, use value from above, plus the id col */
        putReq.setValue(value.put("id1", 1));
        try {
            putRet = handle.put(putReq);
            fail("Exception should have been thrown on put");
        } catch (Exception e) {
            /* success */
        }

        /* try an insert query */
        String insertQ = "insert into tIdentity(id, name) values(5, 'fred')";
        try (QueryRequest qReq = new QueryRequest()) {
            qReq.setStatement(insertQ);
            QueryResult qRes = handle.query(qReq);
            for (MapValue res : qRes.getResults()) {
                assertEquals(1, res.get("NumRowsInserted").getInt());
            }
        }

        insertQ = "insert into tIdentity(id, name) values(5, 'jack')";

        PrepareRequest prepReq = new PrepareRequest().setStatement(insertQ);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();
        try (QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(prepStmt);
            QueryResult qRes = handle.query(qReq);
            for (MapValue res : qRes.getResults()) {
                assertEquals(1, res.get("NumRowsInserted").getInt());
            }
        }
    }

    @Test
    public void testLargeRow() {
        try {
            doLargeRow(handle, false);
        } catch (Exception e) {
            /* success */
        }
        try {
            doLargeRow(handle, true);
        } catch (Exception e) {
            /* success */
        }
    }

   /*
     * Tests support for flexible casting of types in the proxy where there is
     * no data loss, e.g.:
     *  String "1" to Integer (or other numeric)
     *  String "true" or "false" to Boolean
     *  Valid timestamp mappings
     */
    @Test
    public void testFlexibleMapping() throws Exception {
        assumeKVVersion("testFlexibleMapping", 20, 2, 1);
        final String createTable =
            "create table flex(id integer, primary key(id), " +
            "str string, " +
            "bool boolean, " +
            "int integer, " +
            "long long, " +
            "doub double, " +
            "num number, " +
            "ts timestamp(3))";

        /* JSON with various valid mappings */

        /* string value for numeric fields */
        final String strToNum = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": \"5\", " +
            "\"long\": \"456\", \"doub\":\"5.6\", \"num\":\"12345678910\", " +
            "\"ts\": \"2017-08-21T13:34:35.123\"" +
            "}";

        /* int timestamp */
        final String intToTs = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 12" +
            "}";

        /* long timestamp */
        final String longToTs = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": true, \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 1234567891011" +
            "}";

        /* string boolean */
        final String strToBool = "{" +
            "\"id\": 1, \"str\": \"str\", \"bool\": \"true\", \"int\": 5, " +
            "\"long\": 456, \"doub\":5.6, \"num\":12345678910, " +
            "\"ts\": 1234567891011" +
            "}";

        final String[] mappings = {strToNum, intToTs, longToTs, strToBool};

        tableOperation(handle, createTable, new TableLimits(100, 100, 1));

        for (String s : mappings) {
            PutRequest pr = new PutRequest().setValueFromJson(s, null).
                setTableName("flex");
            PutResult pres = handle.put(pr);
            assertNotNull(pres.getVersion());
        }
    }

    private void checkModTime(long modTime, boolean modTimeRecent) {
        if (modTimeRecent) {
            if (modTime < (System.currentTimeMillis() - 2000)) {
                fail("Expected modtime to be recent, got " + modTime);
            }
        } else {
            if (modTime != 0) {
                fail("Expected modtime to be zero, got " + modTime);
            }
        }
    }

    private void checkPutResult(PutRequest request,
                                PutResult result,
                                boolean shouldSucceed,
                                boolean rowPresent,
                                MapValue expPrevValue,
                                Version expPrevVersion,
                                boolean modTimeRecent,
                                int recordKB) {
        if (shouldSucceed) {
            assertNotNull("Put should succeed", result.getVersion());
        } else {
            assertNull("Put should fail", result.getVersion());
        }
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
                                  expPrevValue, expPrevVersion);

        checkModTime(result.getExistingModificationTime(), modTimeRecent);
    }

    private void checkDeleteResult(DeleteRequest request,
                                   DeleteResult result,
                                   boolean shouldSucceed,
                                   boolean rowPresent,
                                   MapValue expPrevValue,
                                   Version expPrevVersion,
                                   boolean modTimeRecent,
                                   int recordKB) {

        assertEquals("Delete should " + (shouldSucceed ? "succeed" : " fail"),
                     shouldSucceed, result.getSuccess());
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
                                  expPrevValue, expPrevVersion);
        checkModTime(result.getExistingModificationTime(), modTimeRecent);
    }

    private void checkGetResult(GetRequest request,
                                GetResult result,
                                boolean rowPresent,
                                MapValue expValue,
                                Version expVersion,
                                boolean modTimeRecent,
                                int recordKB) {


        if (rowPresent) {
            if (expValue != null) {
                assertEquals("Unexpected value", expValue, result.getValue());
            } else {
                assertNotNull("Unexpected value", expValue);
            }
            if (expVersion != null) {
                assertArrayEquals("Unexpected version",
                                  expVersion.getBytes(),
                                  result.getVersion().getBytes());
            } else {
                assertNotNull("Unexpected version", result.getVersion());
            }
        } else {
            assertNull("Unexpected value", expValue);
            assertNull("Unexpected version", result.getVersion());
        }
        checkModTime(result.getModificationTime(), modTimeRecent);
    }

    private void checkExistingValueVersion(WriteRequest request,
                                           WriteResult result,
                                           boolean shouldSucceed,
                                           boolean rowPresent,
                                           MapValue expPrevValue,
                                           Version expPrevVersion) {

        boolean hasReturnRow = rowPresent;
        if (hasReturnRow) {
            assertNotNull("PrevValue should be non-null",
                          result.getExistingValueInternal());
            if (expPrevValue != null) {
                assertEquals("Unexpected PrevValue",
                    expPrevValue, result.getExistingValueInternal());
            }
            assertNotNull("PrevVersion should be non-null",
                          result.getExistingVersionInternal());
            if (expPrevVersion != null) {
                assertNotNull(result.getExistingVersionInternal());
                assertArrayEquals("Unexpected PrevVersion",
                          expPrevVersion.getBytes(),
                          result.getExistingVersionInternal().getBytes());
            }
        } else {
            assertNull("PrevValue should be null",
                       result.getExistingValueInternal());
            assertNull("PrevVersion should be null",
                       result.getExistingVersionInternal());
        }
    }

    private void runPut(String tableName,
                        String targetField,
                        FieldValue[] values,
                        boolean expSucceed) {
        MapValue row = new MapValue().put("id", 1);
        for (FieldValue value : values) {
            row.put(targetField, value);

            PutRequest putReq = new PutRequest()
                .setTableName(tableName)
                .setValue(row);
            try {
                handle.put(putReq);
                if (!expSucceed) {
                    fail("Expected put to fail");
                }
            } catch (Throwable ex) {
                if (expSucceed) {
                    fail("Expected put to succeed");
                }
            }
        }
    }
}
