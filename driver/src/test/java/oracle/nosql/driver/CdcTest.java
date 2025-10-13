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

    private static final String tableName = "JavaTestUsers";

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

        Consumer consumer = null;
        try {
            /* Create a table */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists " + tableName +
                "(id integer, name string, primary key(id))",
                new TableLimits(500, 500, 50));
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Enable CDC on table */
            handle.enableCDC(tableName, null, true, 10000, 500);

            /* create CDC consumer */
            consumer = new ConsumerBuilder()
                .addTable(tableName, null, StartLocation.latest())
                .groupId("test_group")
                .commitAutomatic()
                .handle(handle)
                .build();

            boolean cloudsimCDC = isCloudsimCDC(consumer);

            if (!cloudsimCDC) {
                /* give producer a bit of time to set up table */
                Thread.sleep(2000);
            }

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
            /* Disable CDC on table */
            handle.enableCDC(tableName, null, false, 10000, 500);
        }
    }

    private void pollAndCheckEvent(Consumer consumer,
                                   String tableOcid,
                                   MapValue expKey,
                                   MapValue expValue) {
        // Poll until we get this event back, then verify the returned CDC event matches
        // the record that was written.
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

        assertEquals(message.getTableOcid(), tableOcid);

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

}
