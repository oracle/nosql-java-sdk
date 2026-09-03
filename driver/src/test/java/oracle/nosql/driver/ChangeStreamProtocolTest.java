/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.COMPARTMENT_OCID;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_BUNDLE;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_EVENTS;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_ID;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.TABLE_NAME;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.TABLE_OCID;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import oracle.nosql.driver.Nson.NsonSerializer;
import oracle.nosql.driver.changestream.PollRequest;
import oracle.nosql.driver.changestream.PollResult;
import oracle.nosql.driver.changestream.Record;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory.PollRequestSerializer;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

public class ChangeStreamProtocolTest {

    @Test
    public void testProtocolMessagesAreFlattenedIntoEventBundle()
        throws IOException {
        byte[] firstEvent = createEvent("event-1");
        byte[] secondEvent = createEvent("event-2");
        byte[] response = createPollResponse(firstEvent, secondEvent);
        PollRequest request = new PollRequest(new byte[] {1}, 2);
        PollResult result = (PollResult)new PollRequestSerializer().deserialize(
            request, NettyByteInputStream.createFromBytes(response), (short)4);

        assertEquals(2, result.bundle.getEvents().size());
        Record firstRecord = result.bundle.getEvents().get(0)
            .getRecords().get(0);
        assertEquals("table1", firstRecord.getTableName());
        assertEquals("ocid1.table1", firstRecord.getTableOcid());
        assertEquals("ocid1.compartment1",
                     firstRecord.getCompartmentOcid());
        assertEquals("event-1", firstRecord.getEventId());

        Record secondRecord = result.bundle.getEvents().get(1)
            .getRecords().get(0);
        assertEquals("table2", secondRecord.getTableName());
        assertEquals("ocid1.table2", secondRecord.getTableOcid());
        assertEquals("ocid1.compartment2",
                     secondRecord.getCompartmentOcid());
        assertEquals("event-2", secondRecord.getEventId());
    }

    private static byte[] createEvent(String eventId) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        try {
            NsonSerializer ns = new NsonSerializer(
                new NettyByteOutputStream(buffer));
            ns.startMap(0);
            startArrayField(ns, EVENT_EVENTS);
            ns.startMap(0);
            writeStringField(ns, EVENT_ID, eventId);
            ns.endMap(0);
            ns.endArrayField(0);
            endArrayField(ns, EVENT_EVENTS);
            ns.endMap(0);
            return copyBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    private static byte[] createPollResponse(byte[] firstEvent,
                                             byte[] secondEvent)
        throws IOException {

        ByteBuf buffer = Unpooled.buffer();
        try {
            NsonSerializer ns = new NsonSerializer(
                new NettyByteOutputStream(buffer));
            ns.startMap(0);
            startArrayField(ns, EVENT_BUNDLE);
            writeMessage(ns, firstEvent, "table1", "ocid1.table1",
                         "ocid1.compartment1");
            writeMessage(ns, secondEvent, "table2", "ocid1.table2",
                         "ocid1.compartment2");
            endArrayField(ns, EVENT_BUNDLE);
            ns.endMap(0);
            return copyBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    private static void writeMessage(NsonSerializer ns,
                                     byte[] event,
                                     String tableName,
                                     String tableOcid,
                                     String compartmentOcid)
        throws IOException {

        ns.startMap(0);
        startArrayField(ns, EVENT_EVENTS);
        ns.binaryValue(event);
        ns.endArrayField(0);
        endArrayField(ns, EVENT_EVENTS);
        /*
         * Deliberately write the table fields after EVENT_EVENTS to verify
         * that protocol field ordering does not affect the Record model.
         */
        writeStringField(ns, TABLE_NAME, tableName);
        writeStringField(ns, TABLE_OCID, tableOcid);
        writeStringField(ns, COMPARTMENT_OCID, compartmentOcid);
        ns.endMap(0);
        ns.endArrayField(0);
    }

    private static void startArrayField(NsonSerializer ns, String name)
        throws IOException {

        ns.startMapField(name);
        ns.startArray(0);
    }

    private static void endArrayField(NsonSerializer ns, String name)
        throws IOException {

        ns.endArray(0);
        ns.endMapField(name);
    }

    private static void writeStringField(NsonSerializer ns,
                                         String name,
                                         String value)
        throws IOException {

        ns.startMapField(name);
        ns.stringValue(value);
        ns.endMapField(name);
    }

    private static byte[] copyBytes(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(0, bytes);
        return bytes;
    }
}
