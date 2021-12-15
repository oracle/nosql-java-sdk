/*-
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

/*
 * Put all V4/NSON serialization code here for now, then split it up
 */

package oracle.nosql.driver.ops.serde.nson;

import static oracle.nosql.driver.ops.serde.BinaryProtocol.mapException;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.*;
import static oracle.nosql.driver.util.BinaryProtocol.ABSOLUTE;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_WRITE_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_ALL;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NONE;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SIMPLE_MAJORITY;
import static oracle.nosql.driver.util.BinaryProtocol.EVENTUAL;

import java.io.IOException;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.Nson;
import oracle.nosql.driver.Nson.NsonSerializer;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.FieldFinder;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.ReadRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;

import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.values.MapValue;

public class NsonSerializerFactory implements SerializerFactory {
    static private NsonSerializerFactory factory = new NsonSerializerFactory();

    /* return the singleton */
    public static SerializerFactory getFactory() {
        return factory;
    }

    static final Serializer delSerializer =
        new DeleteRequestSerializer();
    static final Serializer getSerializer =
        new GetRequestSerializer();
    static final Serializer putSerializer =
        new PutRequestSerializer();
    static final Serializer tableSerializer =
        new TableRequestSerializer();
    static final Serializer getTableSerializer =
        new GetTableRequestSerializer();

    // TODO
    static final Serializer querySerializer =
        null; //new QueryV4RequestSerializer();
    static final Serializer prepareSerializer =
        null; //new PrepareV4RequestSerializer();
    static final Serializer getTableUsageSerializer =
        null; //new TableUsageV4RequestSerializer();
    static final Serializer systemSerializer =
        null; //new SystemV4RequestSerializer();
    static final Serializer systemStatusSerializer =
        null; //new SystemStatusV4RequestSerializer();
    static final Serializer listTablesSerializer =
        null; //new ListTablesV4RequestSerializer();
    static final Serializer getIndexesSerializer =
        null; //new GetIndexesV4RequestSerializer();
    static final Serializer writeMultipleSerializer =
        new WriteMultipleRequestSerializer();
    static final Serializer multiDeleteSerializer =
        null; //new MultiDeleteV4RequestSerializer();

    @Override
    public Serializer createDeleteSerializer() {
        return delSerializer;
    }

    @Override
    public Serializer createGetSerializer() {
        return getSerializer;
    }

    @Override
    public Serializer createPutSerializer() {
        return putSerializer;
    }

    @Override
    public Serializer createQuerySerializer() {
        return querySerializer;
    }

    @Override
    public Serializer createPrepareSerializer() {
        return prepareSerializer;
    }

    @Override
    public Serializer createGetTableSerializer() {
        return getTableSerializer;
    }

    @Override
    public Serializer createGetTableUsageSerializer() {
        return getTableUsageSerializer;
    }

    @Override
    public Serializer createListTablesSerializer() {
        return listTablesSerializer;
    }

    @Override
    public Serializer createGetIndexesSerializer() {
        return getIndexesSerializer;
    }

    @Override
    public Serializer createTableOpSerializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createSystemOpSerializer() {
        return systemSerializer;
    }

    @Override
    public Serializer createSystemStatusSerializer() {
        return systemStatusSerializer;
    }

    @Override
    public Serializer createWriteMultipleSerializer() {
        return writeMultipleSerializer;
    }

    @Override
    public Serializer createMultiDeleteSerializer() {
        return multiDeleteSerializer;
    }

    /* deserializers */
    @Override
    public Serializer createDeleteDeserializer() {
        return delSerializer;
    }

    @Override
    public Serializer createGetDeserializer() {
        return getSerializer;
    }

    @Override
    public Serializer createPutDeserializer() {
        return putSerializer;
    }

    @Override
    public Serializer createQueryDeserializer() {
        return querySerializer;
    }

    @Override
    public Serializer createPrepareDeserializer() {
        return prepareSerializer;
    }

    @Override
    public Serializer createGetTableDeserializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createGetTableUsageDeserializer() {
        return getTableUsageSerializer;
    }

    @Override
    public Serializer createListTablesDeserializer() {
        return listTablesSerializer;
    }

    @Override
    public Serializer createGetIndexesDeserializer() {
        return getIndexesSerializer;
    }

    @Override
    public Serializer createTableOpDeserializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createSystemOpDeserializer() {
        return systemSerializer;
    }

    @Override
    public Serializer createSystemStatusDeserializer() {
        return systemStatusSerializer;
    }

    @Override
    public Serializer createWriteMultipleDeserializer() {
        return writeMultipleSerializer;
    }

    @Override
    public Serializer createMultiDeleteDeserializer() {
        return multiDeleteSerializer;
    }


    /* serializers */

    /**
     * Table request:
     *  Payload:
     *    table name (if needed)
     *    statement (DDL)
     *    limits (if required -- create/alter)
     *
     * Table result (all optional):
     *  table name (string)
     *  state (int)
     *  domain id (int)
     *  throughput info (read/write/storage)
     *  schema (string)
     *  operation id (plan id, etc) (int)
     */
    public static class TableRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            TableRequest rq = (TableRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.TABLE_REQUEST.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, STATEMENT, rq.getStatement());
            writeLimits(ns, rq.getTableLimits());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            return deserializeTableResult(request, in);
        }
    }

    /**
     * Get Table request:
     *  Payload:
     *    table name
     *    operation id? (string)
     *
     * Table result (all optional):
     *  table name (string)
     *  state (int)
     *  domain id (int)
     *  throughput info (read/write/storage)
     *  schema (string)
     *  operation id (plan id, etc) (int)
     */
    public static class GetTableRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            GetTableRequest rq = (GetTableRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.GET_TABLE.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, TABLE_NAME, rq.getTableName());
            writeMapField(ns, OPERATION_ID, rq.getOperationId());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            return deserializeTableResult(request, in);
        }
    }

    /**
     * Get request:
     *  Payload:
     *    table name
     *    consistency
     *    key (an NSON object)
     *
     * Get result (all optional):
     *  consumed capacity
     *  meta: mod time, expiration, version
     *  value
     */
    public static class GetRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            GetRequest rq = (GetRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.GET.ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeReadRequest(ns, rq);
            /* writeKey uses the output stream directly */
            writeKey(ns, rq, out);
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            GetResult result = new GetResult();

            FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(ROW)) {
                    readRow(in, result);
                } else {
                    // log/warn
                    walker.skip();
                }
            }
            return result;
        }
    }

    /**
     * Delete request:
     *  Payload:
     *    table name
     *    durability
     *    return row
     *    match version?
     *    key
     *
     * Delete result:
     *  consumed capacity
     *  success?
     *  return row info?
     */
    public static class DeleteRequestSerializer extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            DeleteRequest rq = (DeleteRequest) request;
            Version matchVersion = rq.getMatchVersion();
            OpCode opCode = matchVersion != null ? OpCode.DELETE_IF_VERSION :
                OpCode.DELETE;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, opCode.ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeWriteRequest(ns, rq);

            /* shared with WriteMultiple */
            serializeInternal(rq, ns, out);

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            DeleteResult result = new DeleteResult();

            in.setOffset(0);
            FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(SUCCESS)) {
                    result.setSuccess(Nson.readNsonBoolean(in));
                } else if (name.equals(RETURN_INFO)) {
                    readReturnInfo(in, result);
                } else {
                    // log/warn
                    walker.skip();
                }
            }
            return result;
        }

        /* shared with WriteMultiple */
        void serializeInternal(DeleteRequest rq, NsonSerializer ns,
                               ByteOutputStream out)
            throws IOException {

            if (rq.getMatchVersion() != null) {
                writeMapField(ns, ROW_VERSION, rq.getMatchVersion().getBytes());
            }

            /* writeValue uses the output stream directly */
            writeKey(ns, rq, out);
        }
    }

    /**
     * Put request:
     *  Payload:
     *    table name
     *    durability
     *    return row
     *    exact match?
     *    identity cache size
     *    update TTL?
     *    TTL?
     *    match version?
     *    identity cache size?
     *    value (an NSON object)
     *    TODO: direct JSON value as string?
     *
     * Put result:
     *  consumed capacity
     *  success?
     *  version on success
     *  return row info?
     *  generated value(s)
     */
    public static class PutRequestSerializer extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            PutRequest rq = (PutRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, getOpCode(rq).ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeWriteRequest(ns, rq);

            /* serialize portion shared with WriteMultiple */
            serializeInternal(rq, ns, out);

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            PutResult result = new PutResult();

            in.setOffset(0);
            FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(ROW_VERSION)) {
                    result.setVersion(Version.createVersion(
                                          Nson.readNsonBinary(in)));
                } else if (name.equals(RETURN_INFO)) {
                    readReturnInfo(in, result);
                } else if (name.equals(GENERATED)) {
                    result.setGeneratedValue((MapValue)Nson.readFieldValue(in));
                } else {
                    // log/warn
                    walker.skip();
                }
            }
            return result;
        }

        /**
         * An internal method shared wit WriteMultiple to serialize the
         * shared parts of the request
         */
        public void serializeInternal(PutRequest rq,
                                      NsonSerializer ns,
                                      ByteOutputStream out)
            throws IOException {

            /*
             * in the interest of efficiency, default these booleans
             * to false:
             *  exact match
             *  update TTL
             */
            if (rq.getExactMatch()) {
                writeMapField(ns, EXACT_MATCH, true);
            }
            if (rq.getUpdateTTL()) {
                writeMapField(ns, UPDATE_TTL, true);
            }
            if (rq.getTTL() != null) {
                /* write TTL as string, e.g. 5 DAYS */
                writeMapField(ns, TTL, rq.getTTL().toString());
            }
            if (rq.getIdentityCacheSize() != 0) {
                writeMapField(ns, IDENTITY_CACHE_SIZE,
                              rq.getIdentityCacheSize());
            }
            if (rq.getMatchVersion() != null) {
                writeMapField(ns, ROW_VERSION, rq.getMatchVersion().getBytes());
            }

            /* writeValue uses the output stream directly */
            writeValue(ns, rq, out);
        }
    }

    /**
     * WriteMultiple request:
     *  Payload:
     *   table name
     *   durability
     *   operations array:
     *    for each delete/write:
     *      abortIfUnsuccessful boolean
     *      the delete or write payload, without tablename or durability
     *
     * WriteMultiple result:
     *  consumed capacity
     *  # use existence of fields as success/fail
     *  "wm_success": [ {result}, {result} ]
     *  "wm_failure": {
     *      "wm_fail_index": int
     *      "wm_fail_result": {}
     *   }
     */
    public static class WriteMultipleRequestSerializer
        extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            WriteMultipleRequest rq = (WriteMultipleRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.WRITE_MULTIPLE.ordinal(), rq);
            endMap(ns, HEADER);

            /*
             * payload
             *
             * IMPORTANT: table name and durability MUST be ordered
             * ahead of the operations or the server can't easily
             * deserialize efficiently
             */
            startMap(ns, PAYLOAD);
            writeMapField(ns, TABLE_NAME, rq.getTableName());
            writeMapField(ns, DURABILITY,
                          getDurability(rq.getDurability()));
            writeMapField(ns, NUM_OPERATIONS,
                          rq.getOperations().size());

            startArray(ns, OPERATIONS);
            for (OperationRequest op : rq.getOperations()) {
                /*
                 * Each operation is a map in the array.
                 * Calling the generic put or delete serializer will add
                 * redundant, unneccessary state. In order to share code
                 * with those serializers they have internal methods that
                 * write just what WriteMultiple requires. The exception
                 * is the op code and return row information, so write
                 * that here, then call the shared methods.
                 */
                ns.startMap(0);
                WriteRequest wr = op.getRequest();
                if (op.isAbortIfUnsuccessful()) {
                    writeMapField(ns, ABORT_ON_FAIL,
                                  op.isAbortIfUnsuccessful());
                }

                /*
                 * write op first -- this is important!
                 */
                if (wr instanceof PutRequest) {
                    PutRequest prq = (PutRequest) wr;
                    writeMapField(ns, OP, getOpCode(prq).ordinal());
                    ((PutRequestSerializer)putSerializer).
                        serializeInternal(prq, ns, out);
                } else {
                    DeleteRequest drq = (DeleteRequest) wr;
                    OpCode opCode = drq.getMatchVersion() != null ?
                        OpCode.DELETE_IF_VERSION : OpCode.DELETE;
                    writeMapField(ns, OP, opCode.ordinal());
                    ((DeleteRequestSerializer)delSerializer).
                        serializeInternal(drq, ns, out);
                }
                /* common to both delete and put */
                writeMapField(ns, RETURN_ROW, wr.getReturnRowInternal());
                ns.endMap(0);
                ns.endArrayField(0);
            }
            endArray(ns, OPERATIONS);
            endMap(ns, PAYLOAD);

            /* NOTE: the binary serializer checks request size, should we? */

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            WriteMultipleResult  result = new WriteMultipleResult();

            in.setOffset(0);
            FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(WM_SUCCESS)) {
                    /* success is an array of map */
                    int t = in.readByte();
                    if (t != Nson.TYPE_ARRAY) {
                        throw new IllegalStateException(
                            "Operations: bad type in writemultiple: " +
                            Nson.typeString(t) + ", should be ARRAY");
                    }
                    int totalLength = in.readInt();
                    int numElements = in.readInt();
                    for (int i = 0; i < numElements; i++) {
                        result.addResult(createOperationResult(in));
                    }
                } else if (name.equals(WM_FAILURE)) {
                    /* failure is a map */
                    FieldFinder.MapWalker fw = new FieldFinder.MapWalker(in);
                    while (fw.hasNext()) {
                        fw.next();
                        String fname = fw.getCurrentName();
                        if (fname.equals(WM_FAIL_INDEX)) {
                            result.setFailedOperationIndex(
                                Nson.readNsonInt(in));
                        } else if (fname.equals(WM_FAIL_RESULT)) {
                            /* this is another map */
                            // TODO
                            fw.skip();
                        } else {
                            fw.skip();
                        }
                    }
                } else {
                    // log/warn
                    walker.skip();
                }
            }
            return result;
        }

        /*
         * map structure:
         * "success": bool
         * "version": binary
         * "generated":
         * "return_info":
         */
        private static OperationResult createOperationResult(
            ByteInputStream in) throws IOException {
            OperationResult opResult = new OperationResult();

            FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(SUCCESS)) {
                    opResult.setSuccess(Nson.readNsonBoolean(in));
                } else if (name.equals(ROW_VERSION)) {
                    opResult.setVersion(Version.createVersion(
                                            Nson.readNsonBinary(in)));
                } else if (name.equals(GENERATED)) {
                    walker.skip();
                } else if (name.equals(RETURN_INFO)) {
                    readReturnInfo(in, opResult);
                } else {
                    walker.skip();
                }
            }
            return opResult;
        }
    }

    /*
     * Shared code to deserialize a TableResult
     */
    static protected TableResult deserializeTableResult(Request request,
                                                        ByteInputStream in)
         throws IOException {

        TableResult result = new TableResult();

        in.setOffset(0);

        FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(ERROR_CODE)) {
                handleErrorCode(walker);
            } else if (name.equals(COMPARTMENT_OCID)) {
                result.setCompartmentId(Nson.readNsonString(in));
            } else if (name.equals(NAMESPACE)) {
                result.setNamespace(Nson.readNsonString(in));
            } else if (name.equals(TABLE_OCID)) {
                result.setTableId(Nson.readNsonString(in));
            } else if (name.equals(TABLE_NAME)) {
                result.setTableName(Nson.readNsonString(in));
            } else if (name.equals(TABLE_STATE)) {
                result.setState(TableResult.State.valueOf(
                                    Nson.readNsonString(in)));
            } else if (name.equals(TABLE_SCHEMA)) {
                result.setSchema(Nson.readNsonString(in));
            } else if (name.equals(OPERATION_ID)) {
                result.setOperationId(Nson.readNsonString(in));
            } else if (name.equals(LIMITS)) {
                FieldFinder.MapWalker lw = new FieldFinder.MapWalker(in);
                int ru = 0;
                int wu = 0;
                int sg = 0;
                while (lw.hasNext()) {
                    lw.next();
                    name = lw.getCurrentName();
                    if (name.equals(READ_UNITS)) {
                        ru = Nson.readNsonInt(in);
                    } else if (name.equals(WRITE_UNITS)) {
                        wu = Nson.readNsonInt(in);
                    } else if (name.equals(STORAGE_GB)) {
                        sg = Nson.readNsonInt(in);
                    } else {
                        // log/warn
                        lw.skip();
                    }
                }
                result.setTableLimits(new TableLimits(
                                          ru, wu, sg));
            } else {
                // log/warn
                walker.skip();
            }
        }
        return result;
    }

    /*
     * From here down utilities to handle portions of requests or results
     */
    private static OpCode getOpCode(PutRequest req) {
        if (req.getOption() == null) {
            return OpCode.PUT;
        }
        switch (req.getOption()) {
        case IfAbsent:
            return OpCode.PUT_IF_ABSENT;
        case IfPresent:
            return OpCode.PUT_IF_PRESENT;
        case IfVersion:
            return OpCode.PUT_IF_VERSION;
        default:
            throw new IllegalStateException("Unknown Options " +
                req.getOption());
        }
    }

    /**
     * Base class that implements common methods for serialization and
     * deserialization of V4 protocol
     */
    public static abstract class NsonSerializerBase implements Serializer {

        /**
         * Header
         *  version (int)
         *  operation (int)
         *  timeout (int)
         * tableName was considered but it is part of the payloads. Maybe
         * reconsider for single table ops... we'll see.
         */
        protected void writeHeader(NsonSerializer ns, int op, Request rq)
            throws IOException {

            writeMapField(ns, VERSION, V4_VERSION);
            writeMapField(ns, OP, op);
            writeMapField(ns, TIMEOUT, rq.getTimeoutInternal());
        }

        /**
         * Writes common fields for read requests -- table name and
         * consistency (int)
         */
        protected void writeReadRequest(NsonSerializer ns,
                                        ReadRequest rq)
            throws IOException {

            writeMapField(ns, TABLE_NAME, rq.getTableName());
            writeMapField(ns, CONSISTENCY,
                          getConsistency(rq.getConsistencyInternal()));
        }

        /**
         * Writes common fields for write requests -- table name,
         * durability, return row
         */
        protected void writeWriteRequest(NsonSerializer ns,
                                         WriteRequest rq)
            throws IOException {

            writeMapField(ns, TABLE_NAME, rq.getTableName());
            writeMapField(ns, DURABILITY,
                          getDurability(rq.getDurability()));
            writeMapField(ns, RETURN_ROW, rq.getReturnRowInternal());
        }

        /**
         * Writes a primary key:
         *  "key": {...}
         * The request may be a GetRequest or a DeleteRequest
         */
        protected void writeKey(NsonSerializer ns, Request rq,
                                ByteOutputStream out) throws IOException {

            MapValue key = (rq instanceof GetRequest ?
                            ((GetRequest)rq).getKey() :
                            ((DeleteRequest)rq).getKey());

            ns.startMapField(KEY);
            Nson.writeFieldValue(out, key);
            ns.endMapField(KEY);
        }

        /**
         * Writes a row value:
         *  "value": {...}
         * The request must be a PutRequest
         */
        protected void writeValue(NsonSerializer ns, PutRequest rq,
                                  ByteOutputStream out) throws IOException {

            ns.startMapField(VALUE);
            Nson.writeFieldValue(out, rq.getValue());
            ns.endMapField(VALUE);
        }

        protected void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     int value) throws IOException {
            ns.startMapField(fieldName);
            ns.integerValue(value);
            ns.endMapField(fieldName);
        }

        protected void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     String value) throws IOException {
            ns.startMapField(fieldName);
            ns.stringValue(value);
            ns.endMapField(fieldName);
        }

        protected void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     boolean value) throws IOException {
            ns.startMapField(fieldName);
            ns.booleanValue(value);
            ns.endMapField(fieldName);
        }

        protected void writeMapField(NsonSerializer ns,
                                     String fieldName,
                                     byte[] value) throws IOException {
            ns.startMapField(fieldName);
            ns.binaryValue(value);
            ns.endMapField(fieldName);
        }

        protected void startMap(NsonSerializer ns, String name)
            throws IOException {
            ns.startMapField(name);
            ns.startMap(0);
        }

        protected void endMap(NsonSerializer ns, String name)
            throws IOException {
            ns.endMap(0);
            ns.endMapField(name);
        }

        protected void startArray(NsonSerializer ns, String name)
            throws IOException {
            ns.startMapField(name);
            ns.startArray(0);
        }

        protected void endArray(NsonSerializer ns, String name)
            throws IOException {
            ns.endArray(0);
            ns.endMapField(name);
        }

        protected void writeLimits(NsonSerializer ns, TableLimits limits)
            throws IOException {
            if (limits != null) {
                startMap(ns, LIMITS);
                writeMapField(ns, READ_UNITS, limits.getReadUnits());
                writeMapField(ns, WRITE_UNITS, limits.getWriteUnits());
                writeMapField(ns, STORAGE_GB, limits.getStorageGB());
                endMap(ns, LIMITS);
            }
        }

        private static int getConsistency(Consistency consistency) {
            if (consistency == Consistency.ABSOLUTE) {
                return ABSOLUTE;
            }
            return EVENTUAL;
        }

        public static int getDurability(Durability durability) {
            if (durability == null) {
                return 0;
            }
            int dur = 0;
            switch (durability.getMasterSync()) {
            case NO_SYNC:
                dur = DURABILITY_NO_SYNC;
                break;
            case SYNC:
                dur = DURABILITY_SYNC;
                break;
            case WRITE_NO_SYNC:
                dur = DURABILITY_WRITE_NO_SYNC;
                break;
            }
            switch (durability.getReplicaSync()) {
            case NO_SYNC:
                dur |= DURABILITY_NO_SYNC << 2;
                break;
            case SYNC:
                dur |= DURABILITY_SYNC << 2;
                break;
            case WRITE_NO_SYNC:
                dur |= DURABILITY_WRITE_NO_SYNC << 2;
                break;
            }
            switch (durability.getReplicaAck()) {
            case ALL:
                dur |= DURABILITY_ALL << 4;
                break;
            case NONE:
                dur |= DURABILITY_NONE << 4;
                break;
            case SIMPLE_MAJORITY:
                dur |= DURABILITY_SIMPLE_MAJORITY << 4;
                break;
            }
            return dur;
        }
    }

    /**
     * Handle success/failure in a response. Success is a 0 error code.
     * Failure is a non-zero code and may also include:
     *  Exception message
     *  Consumed capacity
     *  Retry hints if throttling (future)
     * This method throws an appropriately mapped exception on error and
     * nothing on success.
     *
     *   "error_code": int (code)
     *   "exception": "..."
     *   "consumed": {
     *      "read_units": int,
     *      "read_kb": int,
     *      "write_kb": int
     *    }
     *
     * The walker must be positions at the very first field in the response
     * which *must* be the error code.
     *
     * This method either returns a non-zero error code or throws an
     * exception based on the error code and additional information.
     */
    static int handleErrorCode(FieldFinder.MapWalker walker)
        throws IOException {
        ByteInputStream in = walker.getStream();
        int code = Nson.readNsonInt(in);
        if (code == 0) {
            return 0;
        }
        String message = null;
        RuntimeException re = null;
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (EXCEPTION.equals(name)) {
                message = Nson.readNsonString(in);
                re = mapException(code, message);
            } else if (CONSUMED.equals(name)) {
                /* Exception message must come first */
                if (re != null && re instanceof NoSQLException) {
                    // TODO -- add to exceptions
                    // readConsumedCapacity((NoSQLException) re, in);
                }
                walker.skip();
            } else {
                walker.skip();
            }
        }
        if (re == null) {
            /* this should not happen, but do our best if so */
            re = mapException(code, null);
        }
        throw re;
    }

    /**
     * "consumed": {
     *    "read_units": int,
     *    "read_kb": int,
     *    "write_kb": int
     *  }
     *
     */
    static void readConsumedCapacity(ByteInputStream in,
                                     Result result)
        throws IOException {

        FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(READ_UNITS)) {
                result.setReadUnits(Nson.readNsonInt(in));
            } else if (name.equals(READ_KB)) {
                result.setReadKB(Nson.readNsonInt(in));
            } else if (name.equals(WRITE_KB)) {
                result.setWriteKB(Nson.readNsonInt(in));
            } else {
                // log/warn
                walker.skip();
            }
        }
    }

    /**
     * Reads the row from a get operation which includes row metadata
     * and the value
     */
    static void readRow(ByteInputStream in, GetResult result)
        throws IOException {

        FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(MODIFIED)) {
                result.setModificationTime(Nson.readNsonLong(in));
            } else if (name.equals(EXPIRATION)) {
                result.setExpirationTime(Nson.readNsonLong(in));
            } else if (name.equals(ROW_VERSION)) {
                result.setVersion(Version.createVersion(
                                      Nson.readNsonBinary(in)));
            } else if (name.equals(VALUE)) {
                result.setValue((MapValue)Nson.readFieldValue(in));
            } else {
                // log/warn
                walker.skip();
            }
        }
    }

    /**
     * "return_info": {
     *    "existing_value": {}
     *    "existing_version": byte[]
     *    "existing_mod": long
     *    "existing_expiration": long
     *  }
     *
     */
    static void readReturnInfo(ByteInputStream in,
                               WriteResult result)
        throws IOException {

        FieldFinder.MapWalker walker = new FieldFinder.MapWalker(in);
        while (walker.hasNext()) {
            walker.next();
            String name = walker.getCurrentName();
            if (name.equals(EXISTING_MOD_TIME)) {
                result.setExistingModificationTime(Nson.readNsonLong(in));
            } else if (name.equals(EXISTING_VERSION)) {
                result.setExistingVersion(Version.createVersion(
                                              Nson.readNsonBinary(in)));
            } else if (name.equals(EXISTING_VALUE)) {
                result.setExistingValue((MapValue)Nson.readFieldValue(in));
                /* below requires change to WriteRequest */
                // TODO } else if (name.equals(EXISTING_EXPIRATION)) {
                //result.setExistingExpiration(Nson.readNsonLong(in));
            } else {
                // log/warn
                walker.skip();
            }
        }
    }
}
