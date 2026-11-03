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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
                EventBundle third = consumer.poll(2, Duration.ofSeconds(1));
                assertArrayEquals(second.getCursor(), server.lastPollCursor);
                consumer.commit(Duration.ofSeconds(1));
                assertArrayEquals(third.getCursor(), server.committedCursor);
            } finally {
                consumer.close();
            }
            server.assertHealthy();
        }
    }

    @Test
    public void testCommitModeSurvivesTableChanges() throws Exception {
        for (boolean manual : new boolean[] {true, false}) {
            try (ConsumerServer server = new ConsumerServer();
                 NoSQLHandle handle = server.createHandle()) {
                ConsumerBuilder builder = new ConsumerBuilder().handle(handle)
                    .addTable(EXTERNAL_PARENT_OCID, null, StartLocation.earliest())
                    .maxPollInterval(Duration.ofHours(1));
                if (manual) {
                    builder.commitManual();
                }
                Consumer consumer = builder.build();
                try {
                    consumer.addTable(EXTERNAL_CHILD_OCID, null, StartLocation.earliest());
                    consumer.removeTable(EXTERNAL_CHILD_OCID, null);
                    assertEquals(2, server.updates.size());
                    for (MapValue update : server.updates) {
                        assertEquals(manual, update.get("ma") != null && update.getBoolean("ma"));
                    }
                } finally {
                    consumer.close();
                }
                server.assertHealthy();
            }
        }
    }

    @Test
    public void testExplicitCompartmentSurvivesAllConsumerOperations() throws Exception {
        String compartment = "ocid1.compartment.consumer";
        try (ConsumerServer server = new ConsumerServer();
             NoSQLHandle handle = server.createHandle(compartment)) {
            Consumer consumer = new ConsumerBuilder().handle(handle)
                .compartmentOcid(compartment)
                .addTable(EXTERNAL_PARENT_OCID, null, StartLocation.earliest())
                .maxPollInterval(Duration.ofSeconds(2)).build();
            try {
                assertTrue("keepalive did not reach server", server.keepalive.await(5, TimeUnit.SECONDS));
                consumer.poll(2, Duration.ofSeconds(1)).commit(null);
                consumer.commit(null);
                consumer.reset();
                consumer.addTable(EXTERNAL_CHILD_OCID, "other-compartment", StartLocation.earliest());
                consumer.removeTable(EXTERNAL_CHILD_OCID, "other-compartment");
                assertEquals(1, new ConsumerBuilder().handle(handle)
                    .groupId("g").compartmentOcid(compartment)
                    .getMinimumConsumers(Duration.ofSeconds(1)));
            } finally {
                consumer.close();
            }
            Consumer.deleteGroup(handle, "g", compartment, true);
            server.assertHealthy();
        }
    }

    @Test
    public void testEpochStartTimeIsSerialized() throws Exception {
        try (ConsumerServer server = new ConsumerServer();
             NoSQLHandle handle = server.createHandle()) {
            Consumer consumer = new ConsumerBuilder().handle(handle)
                .addTable(EXTERNAL_PARENT_OCID, null, StartLocation.atTime(0))
                .build();
            try {
                MapValue table = server.created.get("ct").asArray().get(0).asMap();
                assertEquals(0L, table.getLong("se"));
                assertEquals(4, table.getInt("so"));
                byte[] body = server.createBody;
                String wire = new String(body, java.nio.charset.StandardCharsets.ISO_8859_1);
                assertTrue(wire.indexOf("se") < wire.indexOf("so"));
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
        private volatile byte[] lastPollCursor;
        private volatile MapValue created;
        private volatile byte[] createBody;
        private final List<MapValue> updates = new CopyOnWriteArrayList<>();
        private final CountDownLatch keepalive = new CountDownLatch(1);

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
                        lastPollCursor = payload.getBinary("cu");
                        if (payload.getInt("me") == 0) {
                            keepalive.countDown();
                        }
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
                            if (mode == 1) {
                                created = payload;
                                createBody = body;
                            } else {
                                updates.add(payload);
                            }
                            response.put("cu", new byte[] {0});
                            response.put("cm", new MapValue()
                                         .put("revision", ++revision));
                        } else if (mode == 5) {
                            committedCursor = payload.getBinary("cu");
                            response.put("cu", committedCursor);
                        } else if (mode == 6) {
                            response.put("cu", new byte[] {0});
                        } else if (mode == 7) {
                            response.put("mc", 1);
                        } else if (mode != 3 && mode != 4) {
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
            return createHandle(null);
        }

        NoSQLHandle createHandle(String expectedCompartment) {
            NoSQLHandleConfig config = new NoSQLHandleConfig(
                "http://127.0.0.1:" + server.getAddress().getPort());
            config.setAuthorizationProvider(new AuthorizationProvider() {
                @Override
                public String getAuthorizationString(Request request) {
                    if (expectedCompartment != null) {
                        // Principal authentication requires this request field.
                        assertEquals(expectedCompartment, request.getCompartment());
                    }
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
