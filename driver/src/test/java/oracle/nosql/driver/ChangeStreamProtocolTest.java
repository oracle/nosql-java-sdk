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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import oracle.nosql.driver.Nson.NsonSerializer;
import oracle.nosql.driver.changestream.Consumer;
import oracle.nosql.driver.changestream.ConsumerBuilder;
import oracle.nosql.driver.changestream.EventBundle;
import oracle.nosql.driver.changestream.Image;
import oracle.nosql.driver.changestream.StartLocation;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.values.MapValue;
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

    @Test
    public void testImageConstructorRetainsValueAndMetadata() {
        MapValue value = new MapValue().put("name", "Alice");
        MapValue metadata = new MapValue().put("version", 1);
        Image image = new Image(value, metadata);

        assertSame(value, image.getValue());
        assertSame(metadata, image.getMetadata());
        assertFalse(image.isEmpty());
    }

    @Test
    public void testConsumerRetainsMetadataFromCreateAndTableUpdates()
        throws Exception {

        try (ConsumerServer server = new ConsumerServer();
             NoSQLHandle handle = server.createHandle()) {
            Consumer consumer = new ConsumerBuilder()
                .handle(handle)
                .groupId("metadata-test")
                .addTable(EXTERNAL_PARENT_OCID, null, StartLocation.earliest())
                .maxPollInterval(Duration.ofHours(1))
                .build();
            try {
                assertEquals(1, consumer.getMetaData().getInt("revision"));
                consumer.addTable(EXTERNAL_CHILD_OCID, null,
                                  StartLocation.earliest());
                assertEquals(2, consumer.getMetaData().getInt("revision"));
                consumer.removeTable(EXTERNAL_CHILD_OCID, null);
                assertEquals(3, consumer.getMetaData().getInt("revision"));
            } finally {
                consumer.close();
            }
            server.assertHealthy();
        }
    }

    @Test
    public void testBundleCommitUsesItsCursorAfterAnotherPoll()
        throws Exception {

        try (ConsumerServer server = new ConsumerServer();
             NoSQLHandle handle = server.createHandle()) {
            Consumer consumer = new ConsumerBuilder()
                .handle(handle)
                .groupId("bundle-test")
                .addTable(EXTERNAL_PARENT_OCID, null, StartLocation.earliest())
                .commitManual()
                .maxPollInterval(Duration.ofHours(1))
                .build();
            try {
                EventBundle first = consumer.poll(2, Duration.ofSeconds(1));
                EventBundle second = consumer.poll(2, Duration.ofSeconds(1));
                assertFalse(Arrays.equals(first.getCursor(), second.getCursor()));

                first.commit(Duration.ofSeconds(1));
                assertArrayEquals(first.getCursor(), server.committedCursor);
            } finally {
                consumer.close();
            }
            server.assertHealthy();
        }
    }

    /* Exercise the public APIs through HTTP without requiring a CDC server. */
    private static class ConsumerServer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private int revision;
        private int pollCount;
        private volatile byte[] committedCursor;

        ConsumerServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/V2/nosql/data", exchange -> {
                try {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    MapValue request = Nson.readNsonMap(
                        NettyByteInputStream.createFromBytes(
                            Arrays.copyOfRange(body, 2, body.length)));
                    MapValue header = request.get("h").asMap();
                    MapValue payload = request.get("p").asMap();
                    MapValue response = new MapValue().put("e", 0);
                    if (header.getInt("o") == 44) {
                        response = Nson.readNsonMap(
                            NettyByteInputStream.createFromBytes(
                                createPollResponse(
                                    createEvent("event-1", EXTERNAL_PARENT_OCID),
                                    createEvent("event-2", EXTERNAL_CHILD_OCID))));
                        response.put("e", 0);
                        response.put("cu", new byte[] {(byte)++pollCount});
                    } else {
                        int mode = payload.getInt("mm");
                        if (mode == 1 || mode == 2) {
                            response.put("cu", new byte[] {0});
                            response.put("cm", new MapValue()
                                         .put("revision", ++revision));
                        } else if (mode == 5) {
                            committedCursor = payload.getBinary("cu");
                            response.put("cu", committedCursor);
                        } else if (mode != 3) {
                            throw new AssertionError("Unexpected mode: " + mode);
                        }
                    }
                    byte[] bytes;
                    ByteBuf buffer = Unpooled.buffer();
                    try {
                        Nson.writeFieldValue(new NettyByteOutputStream(buffer),
                                             response);
                        bytes = copyBytes(buffer);
                    } finally {
                        buffer.release();
                    }
                    String requestId = exchange.getRequestHeaders()
                        .getFirst("x-nosql-request-id");
                    if (requestId != null) {
                        exchange.getResponseHeaders()
                            .set("x-nosql-request-id", requestId);
                    }
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                    exchange.sendResponseHeaders(500, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        NoSQLHandle createHandle() {
            NoSQLHandleConfig config = new NoSQLHandleConfig(
                "http://127.0.0.1:" + server.getAddress().getPort());
            config.setAuthorizationProvider(new AuthorizationProvider() {
                @Override
                public String getAuthorizationString(Request request) {
                    return "test";
                }
                @Override
                public void close() {}
            });
            return NoSQLHandleFactory.createNoSQLHandle(config);
        }

        void assertHealthy() {
            if (failure.get() != null) {
                throw new AssertionError("Mock server failed", failure.get());
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
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
