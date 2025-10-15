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
import java.util.Map;

import oracle.nosql.driver.Version;

import oracle.nosql.driver.cdc.Consumer;
import oracle.nosql.driver.cdc.ConsumerBuilder;
import oracle.nosql.driver.cdc.Event;
import oracle.nosql.driver.cdc.Message;
import oracle.nosql.driver.cdc.MessageBundle;
import oracle.nosql.driver.cdc.Record;
import oracle.nosql.driver.cdc.StartLocation;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;

import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;

import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

public class CdcTest extends ProxyTestBase {


    /*
     * consumers may have metadata associated with them for debugging/testing purposes.
     * One value indicates if the CDC operations are limited due to using CloudSim.
     * Note this is independent of the "cloudsim" test mode, because that can use
     * either internal proxy cloudsim CDC client, or the real cloud CDC client.
     */
    private boolean isCloudsimCDC(Consumer consumer) {
        MapValue md = consumer.getMetaData();
        if (md != null && md.get("cloudsim") != null) {
            return true;
        }
        return false;
    }

    @Test
    public void smokeTest() {

        assumeFalse(onprem);

        Consumer consumer = null;

        String tableName = "cdcSmoke";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 5),
                10000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable CDC on table: wait up to 10 seconds */
            handle.enableCDC(tableName, null, true, 10000, 500);

            /* create CDC consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.latest())
                .groupId("test_group")
                .commitAutomatic()
                .handle(handle)
                .build();

            /* PUT */
            MapValue key = new MapValue().put("id", 10);
            MapValue value = new MapValue().put("id", 10).put("name", "jane");
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setCompartment("testComp")
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
            /* CDC values do not contain the key fields */
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
            handle.enableCDC(tableName, null, false, 10000, 500);
        }
    }

    @Test
    public void closeOpenTest() {
        /* check that closing a consumer and opening a new one will have the
           new consumer pick up where the old one left off */

        assumeFalse(onprem);
        assumeTrue(Boolean.getBoolean("test.all"));

        Consumer consumer = null;

        String tableName = "cdcCloseOpen";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 50),
                10000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable CDC on table */
            handle.enableCDC(tableName, null, true, 10000, 500);

            /* create CDC consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.earliest())
                .groupId("closeOpen1")
                .commitManual()
                .handle(handle)
                .build();

            boolean cloudsimCDC = isCloudsimCDC(consumer);

            if (!cloudsimCDC) {
                /* give producer a bit of time to set up table */
                Thread.sleep(2000);
            }

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for same records, do not commit */
            pollAndCheckManyEvents(consumer, tableName, 10, 0, 9, false, 10);

            /* close the consumer, without committing */
            consumer.close();
            if (!cloudsimCDC) {
                /* give cdc client a bit of time to process close */
                Thread.sleep(1000);
            }

            /* create a new consumer with the same group */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.firstUncommitted())
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
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
                    .setTableName(tableName);
                PutResult res = handle.put(putRequest);
                assertNotNull(res.getVersion());
            }

            /* poll for all records, committing after each */
            pollAndCheckManyEvents(consumer, tableName, 10, 10, 19, true, 1);

            /* close consumer */
            consumer.close();
            if (!cloudsimCDC) {
                /* give cdc client a bit of time to process close */
                Thread.sleep(1000);
            }

            /* create a new consumer with the same group */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.firstUncommitted())
                .groupId("closeOpen1")
                .commitManual()
                .handle(handle)
                .build();

            /* Put another 10 records */
            for (int i=20; i<30; i++) {
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
            handle.enableCDC(tableName, null, false, 10000, 500);
        }
    }

    @Test
    public void manualCommitTest() {

        assumeFalse(onprem);
        assumeTrue(Boolean.getBoolean("test.all"));

        Consumer consumer = null;

        String tableName = "cdcManualCommit";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 50),
                10000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable CDC on table */
            handle.enableCDC(tableName, null, true, 10000, 500);

            /* create CDC consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.firstUncommitted())
                .groupId("manCom1")
                .commitManual()
                .handle(handle)
                .build();

            boolean cloudsimCDC = isCloudsimCDC(consumer);

            if (!cloudsimCDC) {
                /* give producer a bit of time to set up table */
                Thread.sleep(2000);
            }

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
            handle.enableCDC(tableName, null, false, 10000, 500);
        }
    }

    @Test
    public void autoCommitTest() {

        assumeFalse(onprem);
        assumeTrue(Boolean.getBoolean("test.all"));

        Consumer consumer = null;

        String tableName = "cdcAutoCommit";
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 50),
                10000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable CDC on table */
            handle.enableCDC(tableName, null, true, 10000, 500);

            /* create CDC consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, "testComp", StartLocation.firstUncommitted())
                .groupId("autoCom1")
                .commitAutomatic()
                .handle(handle)
                .build();

            boolean cloudsimCDC = isCloudsimCDC(consumer);

            if (!cloudsimCDC) {
                /* give producer a bit of time to set up table */
                Thread.sleep(2000);
            }

            /* Put 10 records */
            for (int i=0; i<10; i++) {
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
                MapValue key = new MapValue().put("id", i);
                MapValue value = new MapValue().put("id", i).put("name", "jane");
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setCompartment("testComp")
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
            handle.enableCDC(tableName, null, false, 10000, 500);
        }
    }

    private void pollAndCheckEvent(Consumer consumer,
                                   String tableOcid,
                                   MapValue expKey,
                                   MapValue expValue) {
        /*
         * Poll until we get this event back, then verify the returned CDC event matches
         * the record that was written.
         */
        MessageBundle bundle = consumer.poll(1, Duration.ofSeconds(10));
        if (bundle == null || bundle.isEmpty()) {
            fail("Poll returned no results after 10 seconds");
        }
        System.out.println("Received bundle: " + bundle);
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
            System.out.println("Received bundle: " + bundle);
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
}

