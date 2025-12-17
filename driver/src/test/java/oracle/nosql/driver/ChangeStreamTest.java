/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import oracle.nosql.driver.changestream.Consumer;
import oracle.nosql.driver.changestream.ConsumerBuilder;
import oracle.nosql.driver.changestream.Event;
import oracle.nosql.driver.changestream.Message;
import oracle.nosql.driver.changestream.MessageBundle;
import oracle.nosql.driver.changestream.Record;
import oracle.nosql.driver.changestream.StartLocation;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;

import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

public class CdcTest extends ProxyTestBase {

    @Override
    public void beforeTest() throws Exception {
        /*
         * do nothing: test cases will decide if they need to connect.
         * This is to speed up execution when tests are skipped.
         * see myBeforeTest() below.
         */
    }

    /*
     * This exists so when tests are skipped, they don't create handles
     * or manage existing tables, which can take a lot of (sleeping) time if
     * DDL rate limiting is in effect. This call should be in each test case,
     * after it has determined if it going to be skipped.
     */
    private void myBeforeTest() throws Exception {
        /*
         * Configure and get the handle
         */
        handle = getHandle(endpoint);

        /* track existing tables and don't drop them */
        existingTables = new HashSet<String>();
        ListTablesRequest listTables = new ListTablesRequest();
        ddlLimitOp();
        ListTablesResult lres = handle.listTables(listTables);
        proxySerialVersion = lres.getServerSerialVersion();
        for (String tableName: lres.getTables()) {
            existingTables.add(tableName);
        }
    }


    @Test
    public void smokeTest() throws Exception {
        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamSmoke";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table: wait up to 10 seconds */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.latest())
                .groupId("test_group")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* PUT */
            MapValue key = new MapValue().put("id", 10);
            MapValue value = new MapValue().put("id", 10).put("name", "jane");
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);
            PutResult res = handle.put(putRequest);
            assertNotNull(res.getVersion());

            /* GET */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            GetResult res1 = handle.get(getRequest);
            assertNotNull(res1.getJsonValue());

            /* poll for same record */
            /* Change Streaming values do not contain the key fields */
            MapValue expval = new MapValue().put("name", "jane");
            pollAndCheckEvent(consumer, tableName, key, expval);

            /* DELETE */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName(tableName);
            DeleteResult del = handle.delete(delRequest);
            assertTrue(del.getSuccess());

            pollAndCheckEvent(consumer, tableName, key, null);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void closeOpenTest() throws Exception {
        /* check that closing a consumer and opening a new one will have the
           new consumer pick up where the old one left off */

        assumeFalse(onprem);
        // StartLocation.firstUncommitted not yet implemented
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamCloseOpen";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("closeOpen1")
                .commitManual()
                .handle(handle)
                .build();

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for same records, do not commit */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 10);

            /* close the consumer, without committing */
            consumer.close();

            /* create a new consumer with the same group */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.firstUncommitted())
                .groupId("closeOpen1")
                .commitManual()
                .handle(handle)
                .build();

            /* poll for same records, do not commit */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 10);

            /* reset consumer to first uncommitted */
            // TODO: consumer.SOMETHING?

            /* Put another 10 records */
            for (int i=10; i<20; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for all records, committing after each */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, true, 1);

            /* close consumer */
            consumer.close();

            /* create a new consumer with the same group */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.firstUncommitted())
                .groupId("closeOpen1")
                .commitManual()
                .handle(handle)
                .build();

            /* Put another 10 records */
            for (int i=20; i<30; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll should return just the last 10 records */
            pollAndCheckManyEvents(consumer, tableName, 10, 20, 29, false, 10);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void atTimeTest() throws Exception {
        /* check that specifying AT_TIME works properly */

        assumeFalse(onprem);
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamAtTime";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* wait one second */
            Thread.sleep(1000);
            /* get current timestamp */
            long startAt = System.currentTimeMillis();

            /* Put 10 more records */
            for (int i=10; i<20; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.atTime(startAt))
                .groupId("startTime1")
                .commitManual()
                .handle(handle)
                .build();


            /* poll for records, expect only the last 10 */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, false, 10);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void atTimeTest2() throws Exception {
        /*
         * Slight variation of atTimeTest: write 10 records,
         * then create consumer with AT_TIME = now. Then write 10
         * more records and then poll. Verify we only get the last 10.
         */

        assumeFalse(onprem);
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamAtTime2";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* wait one second */
            Thread.sleep(1000);
            /* get current timestamp */
            long startAt = System.currentTimeMillis();

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.atTime(startAt))
                .groupId("startTime2")
                .commitManual()
                .handle(handle)
                .build();

            /* Put 10 more records */
            for (int i=10; i<20; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for records, expect only the last 10 */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, false, 10);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void manualCommitTest() throws Exception {

        assumeFalse(onprem);
        // manual commit not yet implemented
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamManualCommit";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.firstUncommitted())
                .groupId("manCom1")
                .commitManual()
                .handle(handle)
                .build();

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for same records, do not commit */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 10);

            /* reset the consumer */
            consumer.reset();

            /* poll for same records, do not commit */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 10);

            /* Put another 10 records */
            for (int i=10; i<20; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for all records, committing after each */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, true, 1);

            /* reset the consumer */
            consumer.reset();

            /* Put another 10 records */
            for (int i=20; i<30; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll should return just the last 10 records */
            pollAndCheckManyEvents(consumer, tableName, 10, 20, 29, false, 10);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void autoCommitTest() throws Exception {

        assumeFalse(onprem);
        // auto commit not yet implemented
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer = null;

        String tableName = "changestreamAutoCommit";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.firstUncommitted())
                .groupId("autoCom1")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for same records */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 1);

            /* reset the consumer */
            consumer.reset();

            /* poll for same records, should get 1 (last one uncommitted) */
            pollAndCheckManyEvents(consumer, tableName, 1, 0, 9, false, 10);

            /* Put another 10 records */
            for (int i=10; i<20; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for added records */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, false, 1);

            /* reset the consumer */
            consumer.reset();

            /* Put another 10 records */
            for (int i=20; i<30; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll should return just the last 10 records */
            pollAndCheckManyEvents(consumer, tableName, 10, 20, 29, false, 10);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void multipleConsumersTest() throws Exception {

        assumeFalse(onprem);
        // fails because there need to be two stream partitions for there to be
        // the concurrency that this test expects
        assumeTrue(Boolean.getBoolean("test.all"));
        myBeforeTest();

        Consumer consumer1 = null;
        Consumer consumer2 = null;

        String tableName = "changestreamMultiConsumer";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            if (verbose)
                System.out.println("Enable changestream on " + tableName);
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumers */
            consumer1 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("multiCons1")
                .commitAutomatic()
                .handle(handle)
                .build();

            consumer2 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("multiCons1")
                .commitAutomatic()
                .handle(handle)
                .build();

            if (verbose) System.out.println("Created two consumers");


            /* Put 100 records */
            for (int i=0; i<100; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            if (verbose) System.out.println("Finish inserting records");

            // poll from both consumers. Expect to get 100 total, and have somewhat even distribution
            // LQL: Multiple consumers will only get records if there are
            // multiple stream partitions. Right now,  this is configured
            // to one stream partition, so one consumer will get 0 records.
            Map<MapValue, MapValue> records1 = new HashMap<>();
            Map<MapValue, MapValue> records2 = new HashMap<>();

            // wait up to 20 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                if (verbose) System.out.println("Poll for consumer 1");
                pollEvents(consumer1, 5, records1, 5);

                if (verbose) System.out.println("Poll for consumer 2");
                pollEvents(consumer2, 5, records2, 5);

                if (records1.size() + records2.size() == 100) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 200000) {
                    System.out.println("Giving up looking for 100 records " +
                            "after 200 seconds");
                    break;
                }
            } while(true);

            if (verbose) System.out.println(" records1.size()=" + records1.size());
            if (verbose) System.out.println(" records2.size()=" + records2.size());
            int total = records1.size() + records2.size();
            if (total != 100) {
                fail("Expected 100 records total, got " + total +
                     " (records1=" + records1.size() +
                     " records2=" + records2.size() + ")");
            }
            if (records1.size() < 25) {
                fail("Expected at least 25 records for consumer1, got " + records1.size());
            }
            if (records2.size() < 25) {
                fail("Expected at least 25 records for consumer2, got " + records2.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e);
        } finally {
            if (consumer1 != null) {
                consumer1.close();
            }
            if (consumer2 != null) {
                consumer2.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void multipleGroupsTest() throws Exception {

        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer1 = null;
        Consumer consumer2 = null;

        String tableName = "changestreamMultiConsumer";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            enableDisableStreamingWithRateLimiting(handle,tableName, true);

            /* create Change Streaming consumers with different groups */
            consumer1 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("multiGroup1")
                .commitAutomatic()
                .handle(handle)
                .build();

            consumer2 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("multiGroup2")
                .commitAutomatic()
                .handle(handle)
                .build();


            /* Put 100 records */
            for (int i=0; i<100; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            // poll from both consumers. Expect to get 200 total
            Map<MapValue, MapValue> records1 = new HashMap<>();
            Map<MapValue, MapValue> records2 = new HashMap<>();
            // wait up to 20 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer1, 5, records1, 5);
                pollEvents(consumer2, 5, records2, 5);
                if (records1.size() + records2.size() == 200) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 200000) {
                    System.out.println("Giving up looking for 200 records " +
                            "after 200 seconds");
                    break;
                }
            } while(true);

            if (verbose) System.out.println(" records1.size()=" + records1.size());
            if (verbose) System.out.println(" records2.size()=" + records2.size());
            int total = records1.size() + records2.size();
            if (total != 200) {
                fail("Expected 200 records total, got " + total +
                     " (records1=" + records1.size() +
                     " records2=" + records2.size() + ")");
            }
            if (records1.size() != 100) {
                fail("Expected 100 records for consumer1, got " + records1.size());
            }
            if (records2.size() != 100) {
                fail("Expected 100 records for consumer2, got " + records2.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        } finally {
            if (consumer1 != null) {
                consumer1.close();
            }
            if (consumer2 != null) {
                consumer2.close();
            }
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    @Test
    public void multipleTablesTest() throws Exception {

        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer = null;

        String tableName1 = "changestreamMultiTable1";
        String tableName2 = "changestreamMultiTable2";

        try {
            /* Create table1 */
            TableResult tres1 = tableOperation(
                handle,
                "create table if not exists " + tableName1 +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres1.getTableState());

            /* Enable Change Streaming on table1 */
            enableDisableStreamingWithRateLimiting(handle, tableName1, true);

            /* Create table2 */
            TableResult tres2 = tableOperation(
                handle,
                "create table if not exists " + tableName2 +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres2.getTableState());

            /* Enable Change Streaming on table2 */
            enableDisableStreamingWithRateLimiting(handle, tableName2, true);

            /* create Change Streaming consumer for both tables */
            consumer = new ConsumerBuilder()
                .addTable(tableName1, null, StartLocation.earliest())
                .addTable(tableName2, null, StartLocation.earliest())
                .groupId("multiTable1")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* Put 50 records to table1 */
            for (int i=0; i<50; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName1);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* Put 50 records to table2 */
            for (int i=50; i<100; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName2);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            // poll from both tables. Expect to get 100 total
            Map<MapValue, MapValue> records = new HashMap<>();
            // wait up to 20 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer, 5, records, 5);
                if (records.size() == 100) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 200000) {
                    System.out.println("Giving up looking for 100 records " +
                            "after 200 seconds");
                    break;
                }
            } while(true);

            if (records.size() != 100) {
                fail("Expected 100 records total, got " + records.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            /* disable Change Streaming on tables */
            enableDisableStreamingWithRateLimiting(handle, tableName1, false);
            enableDisableStreamingWithRateLimiting(handle, tableName2, false);
        }
    }

    @Test
    public void addRemoveTablesTest() throws Exception {

        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer = null;

        String tableName1 = "changestreamAddRemoveTable1";
        String tableName2 = "changestreamAddRemoveTable2";
        String tableName3 = "changestreamAddRemoveTable3";

        boolean table1Enabled = false;
        boolean table2Enabled = false;
        boolean table3Enabled = false;

        try {
            /* Create table1 */
            TableResult tres1 = tableOperation(
                handle,
                "create table if not exists " + tableName1 +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres1.getTableState());

            /* Enable Change Streaming on table1 */
            enableDisableStreamingWithRateLimiting(handle, tableName1, true);
            table1Enabled = true;

            /* Create table2 */
            TableResult tres2 = tableOperation(
                handle,
                "create table if not exists " + tableName2 +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres2.getTableState());

            /* Enable Change Streaming on table2 */
            enableDisableStreamingWithRateLimiting(handle, tableName2, true);
            table2Enabled = true;

            /* Create table3 */
            TableResult tres3 = tableOperation(
                handle,
                "create table if not exists " + tableName3 +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres3.getTableState());

            /* Enable Change Streaming on table3 */
            enableDisableStreamingWithRateLimiting(handle, tableName3, true);
            table3Enabled = true;

            /* create Change Streaming consumer for table1 */
            consumer = new ConsumerBuilder()
                .addTable(tableName1, null, StartLocation.earliest())
                .groupId("addRemoveTable1")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* Put 50 records to table1 */
            for (int i=0; i<50; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName1);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* Put 50 records to table2 */
            for (int i=50; i<100; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName2);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            // poll. Expect to get 50 total
            Map<MapValue, MapValue> records = new HashMap<>();
            // wait up to 30 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer, 5, records, 5);
                if (records.size() == 50) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 30000) {
                    System.out.println("Giving up looking for 50 records " +
                            "after 30 seconds");
                    break;
                }
            } while(true);

            if (records.size() != 50) {
                fail("Expected 50 records from table1, got " + records.size());
            }

            /* Add table2 to consumer group */
            consumer.addTable(tableName2, null, StartLocation.earliest());

            // poll. Expect to get 50 total (all from table2).
            records.clear();
            // wait up to 30 seconds for all records
            startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer, 5, records, 5);
                if (records.size() == 50) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 30000) {
                    System.out.println("Giving up looking for 50 records " +
                            "after 30 seconds");
                    break;
                }
            } while(true);

            if (records.size() != 50) {
                fail("Expected 50 records from table2, got " + records.size());
            }

            /* Add table3, remove table1 */
            consumer.addTable(tableName3, null, StartLocation.earliest());
            consumer.removeTable(tableName1, null);

            /* Put 50 records to table1 */
            for (int i=100; i<150; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName1);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* Put 50 records to table3 */
            for (int i=200; i<250; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName3);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            // poll. Expect to get 50 total (all from table3).
            records.clear();
            // wait up to 30 seconds for all records
            startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer, 5, records, 5);
                if (records.size() == 50) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 30000) {
                    System.out.println("Giving up looking for 50 records " +
                            "after 30 seconds");
                    break;
                }
            } while(true);

            if (records.size() != 50) {
                fail("Expected 50 records from table3, got " + records.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            /* disable Change Streaming on tables */
            if (table1Enabled) enableDisableStreamingWithRateLimiting(handle, tableName1, false);
            if (table2Enabled) enableDisableStreamingWithRateLimiting(handle, tableName2, false);
            if (table3Enabled) enableDisableStreamingWithRateLimiting(handle, tableName3, false);
        }
    }

    @Test
    public void childTablesTest() throws Exception {

        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer = null;

        String parentTableName = "changestreamParent1";
        String childTableName = "changestreamParent1.child";

        try {
            /* Create parent table */
            TableResult pres = tableOperation(
                handle,
                "create table if not exists " + parentTableName +
                "(sid integer, id integer, name string, primary key(shard(sid), id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, pres.getTableState());

            /* Enable Change Streaming on parent */
// TODO: test only enabling Change Streaming on child
            enableDisableStreamingWithRateLimiting(handle, parentTableName, true);

            /* Create child table */
            TableResult cres = tableOperation(
                handle,
                "create table if not exists " + childTableName +
                "(childid integer, childname string, primary key(childid))",
                null, /* new TableLimits(500, 500, 5),*/
                20000);
            assertEquals(TableResult.State.ACTIVE, cres.getTableState());

            /* Enable Change Streaming on child */
            enableDisableStreamingWithRateLimiting(handle, childTableName, true);

            /* create Change Streaming consumer for both tables */
            consumer = new ConsumerBuilder()
                .addTable(parentTableName, null, StartLocation.earliest())
                .addTable(childTableName, null, StartLocation.earliest())
                .groupId("parentChild1")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* Put 50 records to parent */
            for (int i=0; i<50; i++) {
                MapValue key = new MapValue().put("sid", i).put("id", i);
                MapValue value = new MapValue().put("sid", i).put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(parentTableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* Put 50 records to child */
            for (int i=50; i<100; i++) {
                MapValue value = new MapValue().put("sid", i-50).put("id", i-50)
                                     .put("childid", i).put("childname", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(childTableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            // poll from both tables. Expect to get 100 total
            Map<MapValue, MapValue> records = new HashMap<>();
            // wait up to 60 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                pollEvents(consumer, 5, records, 5);
                if (records.size() == 100) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 60000) {
                    System.out.println("Giving up looking for 100 records " +
                            "after 60 seconds");
                    break;
                }
            } while(true);

            if (records.size() != 100) {
                fail("Expected 100 records total, got " + records.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            /* disable Change Streaming on tables */
            enableDisableStreamingWithRateLimiting(handle, childTableName, false);
            enableDisableStreamingWithRateLimiting(handle, parentTableName,
                    false);
        }
    }

    @Test
    public void deleteGroupTest() throws Exception {

        assumeFalse(onprem);
        myBeforeTest();

        Consumer consumer1 = null;
        Consumer consumer2 = null;

        String tableName = "changestreamDeleteGroup";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable Change Streaming on table */
            if (verbose)
                System.out.println("Enable changestream on " + tableName);
            enableDisableStreamingWithRateLimiting(handle, tableName, true);

            /* create Change Streaming consumers */
            consumer1 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("deleteGroup")
                .commitAutomatic()
                .handle(handle)
                .build();

            consumer2 = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.earliest())
                .groupId("deleteGroup")
                .commitAutomatic()
                .handle(handle)
                .build();

            if (verbose) System.out.println("Created two consumers");


            /* Put 100 records */
            for (int i=0; i<100; i++) {
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            if (verbose) System.out.println("Finish inserting records");

            // poll from both consumers. Get about 20 records.
            Map<MapValue, MapValue> records1 = new HashMap<>();
            Map<MapValue, MapValue> records2 = new HashMap<>();

            // wait up to 20 seconds for all records
            long startTime = System.currentTimeMillis();
            do {
                if (verbose) System.out.println("Poll for consumer 1");
                pollEvents(consumer1, 5, records1, 5);

                if (verbose) System.out.println("Poll for consumer 2");
                pollEvents(consumer2, 5, records2, 5);

                if (records1.size() + records2.size() > 20) {
                    break;
                }
                long now = System.currentTimeMillis();
                if ((now - startTime) > 200000) {
                    System.out.println("Giving up looking for 20 records " +
                            "after 200 seconds");
                    break;
                }
            } while(true);

            /* Use a separate call to delete the entire group */
            Consumer.deleteGroup(handle, "deleteGroup", null);

            /* try again to poll for more records. This should fail. */
            try {
                pollEvents(consumer1, 5, records1, 5);
                fail("Consumer 1 Poll after group delete should have failed");
            } catch (Exception e) {
                /* TODO: check type of exception */
            }

            try {
                pollEvents(consumer2, 5, records1, 5);
                fail("Consumer 2 Poll after group delete should have failed");
            } catch (Exception e) {
                /* TODO: check type of exception */
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e);
        } finally {
            enableDisableStreamingWithRateLimiting(handle, tableName, false);
        }
    }

    private boolean pollEvents(Consumer consumer,
                               int maxEvents,
                               Map<MapValue, MapValue> records,
                               int waitSeconds) {
        MessageBundle bundle = consumer.poll(maxEvents, Duration.ofSeconds(waitSeconds));
        if (bundle == null || bundle.isEmpty()) {
            return false;
        }
        if (verbose) System.out.println("Received bundle: " + bundle);
        for (Message message : bundle.getMessages()) {
            assertNotNull(message.getEvents());
            for (Event event : message.getEvents()) {
                assertNotNull(event.getRecords());
                for (Record record : event.getRecords()) {
                    assertNotNull(record.getCurrentImage());
                    records.put(record.getRecordKey(), record.getCurrentImage().getValue());
                }
            }
        }
        return true;
    }

    private void pollAndCheckEvent(Consumer consumer,
                                   String tableOcid,
                                   MapValue expKey,
                                   MapValue expValue) {
        /*
         * Poll until we get this event back, then verify the returned Change Streaming event matches
         * the record that was written.
         */
        MessageBundle bundle = consumer.poll(1, Duration.ofSeconds(10));
        if (bundle == null || bundle.isEmpty()) {
            fail("Poll returned no results after 10 seconds");
        }
        if (verbose) System.out.println("Received bundle: " + bundle);
        int numMessages = bundle.getMessages().size();
        if (numMessages != 1) {
            fail("Poll returned " + numMessages + " messages, expected 1");
        }

        Message message = bundle.getMessages().get(0);
        assertNotNull(message.getEvents());
        if (message.getEvents().size() != 1) {
            fail("Poll returned " + message.getEvents().size() + " events, expected 1");
        }
        // TODO: check message.TableName against table name (not OCID)
        // TODO: check message.CompartmentOCID when using a different compartment

        // TODO: check OCID for event, only have table name in SDK
        //assertEquals(message.getTableOcid(), tableOcid);

        Event event = message.getEvents().get(0);
        assertNotNull(event.getRecords());
        if (event.getRecords().size() != 1) {
            fail("Event contained " + event.getRecords().size() + " records, expected 1");
        }

        Record record = event.getRecords().get(0);

        // TODO: check EventID?

        assertEquals(expKey, record.getRecordKey());
        if (expValue != null) {
            assertNotNull(record.getCurrentImage());
            assertEquals(expValue, record.getCurrentImage().getValue());
            // TODO record.CurrentImage.RecordMetadata
        } else {
            assertNull(record.getCurrentImage());
        }

        // TODO: beforeImage testing
        //suite.Require().Nil(record.BeforeImage)

        // TODO record.ModificationTime time.Time
        // TODO record.ExpirationTime time.Time
        // TODO record.PartitionID int
        // TODO record.RegionID int
    }

    private void pollAndCheckManyEvents(Consumer consumer, String tableName,
                                        int expNumRecords, int minId, int maxId,
                                        boolean doCommit, int recordsPerPoll) {
        int receivedRecords = 0;
        while (receivedRecords < expNumRecords) {
            int pollMax = recordsPerPoll;
            if (pollMax <= 0) {
                pollMax = expNumRecords - receivedRecords;
            }
            MessageBundle bundle = consumer.poll(pollMax, Duration.ofSeconds(10));
            if (bundle == null || bundle.isEmpty()) {
                fail("Poll returned no results after 10 seconds (received records=" + receivedRecords +")");
            }
            if (verbose) System.out.println("Received bundle: " + bundle);
            int numMessages = bundle.getMessages().size();
            for (int m=0; m<numMessages; m++) {
                Message message = bundle.getMessages().get(m);
                assertNotNull(message.getEvents());
                int numEvents = message.getEvents().size();
                for (int e=0; e<numEvents; e++) {
                    Event event = message.getEvents().get(e);
                    assertNotNull(event.getRecords());
                    int numRecords = event.getRecords().size();
                    for (int r=0; r<numRecords; r++) {
                        Record record = event.getRecords().get(r);
                        MapValue key = record.getRecordKey();
                        int id = key.get("id").getInt();
                        if (id < minId || id > maxId) {
                            fail("Expected " + minId + "<=id<=" + maxId + ", got id=" + id);
                        }
                        receivedRecords++;
                    }
                }
            }
            if (doCommit) {
                consumer.commit(Duration.ofSeconds(1));
            }
        }
        if (receivedRecords != expNumRecords) {
            fail("Expected " + expNumRecords + " records, got " + receivedRecords);
        }
    }

    private void enableDisableStreamingWithRateLimiting(NoSQLHandle handle,
                                                        String tableName,
                                                        boolean enableOrDisable)
            throws InterruptedException {
        int retries = 5;
        while (retries > 0) {
            try {
                ddlLimitOp();
                handle.enableChangeStreaming(tableName, null, enableOrDisable, 20000, 500);
                break;
            } catch (OperationThrottlingException e) {
                if (verbose) {
                    System.out.println("Enabling changestream on table " + tableName +
                            " incurred throttling exception, will retry in 20 " +
                            "seconds: " + e);
                }
                retries--;
                if (retries == 0) {
                    throw e;
                }
                Thread.sleep(20000);
            }
        }
    }
}

