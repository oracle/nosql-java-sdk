/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_BUNDLE;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_EVENTS;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_ID;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_RECORDS;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_REGION_NAME;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.EVENT_TABLE_OCID;
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

    private static final String INTERNAL_PARENT_OCID =
        "ocid1_nosqltable_oc1_us_phoenix_1_parent";
    private static final String EXTERNAL_PARENT_OCID =
        "ocid1.nosqltable.oc1.us-phoenix-1.parent";
    private static final String INTERNAL_CHILD_OCID =
        "ocid1_nosqltable_oc1_us_phoenix_1_child";
    private static final String EXTERNAL_CHILD_OCID =
        "ocid1.nosqltable.oc1.us-phoenix-1.child";
    private static final String EXTERNAL_REGION_NAME = "us-phoenix-1";

    @Test
    public void testInternalRecordTableOcidIsConvertedToExternalFormat()
        throws IOException {

        /*
         * The proxy embeds record data as binary NSON and uses its internal
         * table OCID format there. The SDK must expose only the public form.
         */
        byte[] response = createPollResponse(
            createEvent("event-1", INTERNAL_CHILD_OCID),
            createEvent("event-2", INTERNAL_PARENT_OCID));
        PollRequest request = new PollRequest(new byte[] {1}, 2);
        PollResult result = (PollResult)new PollRequestSerializer().deserialize(
            request, NettyByteInputStream.createFromBytes(response), (short)4);

        Record childRecord = result.bundle.getEvents().get(0)
            .getRecords().get(0);
        assertEquals(EXTERNAL_CHILD_OCID, childRecord.getTableOcid());
        assertEquals("event-1", childRecord.getEventId());

        Record legacyRecord = result.bundle.getEvents().get(1)
            .getRecords().get(0);
        assertEquals(EXTERNAL_PARENT_OCID, legacyRecord.getTableOcid());
        assertEquals("event-2", legacyRecord.getEventId());
    }

    private static byte[] createEvent(String eventId, String tableOcid)
        throws IOException {

        ByteBuf buffer = Unpooled.buffer();
        try {
            NsonSerializer ns = new NsonSerializer(
                new NettyByteOutputStream(buffer));
            ns.startMap(0);
            startArrayField(ns, EVENT_RECORDS);
            ns.startMap(0);
            writeStringField(ns, EVENT_ID, eventId);
            if (tableOcid != null) {
                writeStringField(ns, EVENT_TABLE_OCID, tableOcid);
            }
            ns.endMap(0);
            ns.endArrayField(0);
            endArrayField(ns, EVENT_RECORDS);
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
            writeStringField(ns, EVENT_REGION_NAME, EXTERNAL_REGION_NAME);
            startArrayField(ns, EVENT_BUNDLE);
            ns.startMap(0);
            startArrayField(ns, EVENT_EVENTS);
            ns.binaryValue(firstEvent);
            ns.endArrayField(0);
            ns.binaryValue(secondEvent);
            ns.endArrayField(0);
            endArrayField(ns, EVENT_EVENTS);
            ns.endMap(0);
            ns.endArrayField(0);
            endArrayField(ns, EVENT_BUNDLE);
            ns.endMap(0);
            return copyBytes(buffer);
        } finally {
            buffer.release();
        }
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
