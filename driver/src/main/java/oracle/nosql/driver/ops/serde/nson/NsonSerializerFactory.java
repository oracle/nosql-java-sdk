/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

/*
 * Put all V4/NSON serialization code here for now, then split it up
 */

package oracle.nosql.driver.ops.serde.nson;

import static oracle.nosql.driver.ops.serde.BinaryProtocol.mapException;
import static oracle.nosql.driver.ops.serde.BinaryProtocol.getTableState;
import static oracle.nosql.driver.ops.serde.nson.NsonProtocol.*;
import static oracle.nosql.driver.util.BinaryProtocol.ABSOLUTE;
import static oracle.nosql.driver.util.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.driver.util.BinaryProtocol.COMPLETE;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_WRITE_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_ALL;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NONE;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SIMPLE_MAJORITY;
import static oracle.nosql.driver.util.BinaryProtocol.EVENTUAL;
import static oracle.nosql.driver.util.BinaryProtocol.ON_DEMAND;
import static oracle.nosql.driver.util.BinaryProtocol.PROVISIONED;
import static oracle.nosql.driver.util.BinaryProtocol.UNSUPPORTED_PROTOCOL;
import static oracle.nosql.driver.util.BinaryProtocol.WORKING;

import java.io.IOException;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.DefinedTags;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.FreeFormTags;
import oracle.nosql.driver.Nson;
import oracle.nosql.driver.Nson.NsonSerializer;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapWalker;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetIndexesResult.IndexInfo;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.ReplicaStatsRequest;
import oracle.nosql.driver.ops.ReplicaStatsResult;
import oracle.nosql.driver.ops.ReplicaStatsResult.ReplicaStats;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.TableUsageResult.TableUsage;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.SystemStatusRequest;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.Replica;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;

import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.query.PlanIter;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.query.VirtualScan;
import oracle.nosql.driver.query.VirtualScan.TableResumeInfo;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.values.FieldValue;
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
    static final Serializer querySerializer =
        new QueryRequestSerializer();
    static final Serializer prepareSerializer =
        new PrepareRequestSerializer();
    static final Serializer getTableUsageSerializer =
        new TableUsageRequestSerializer();
    static final Serializer systemSerializer =
        new SystemRequestSerializer();
    static final Serializer systemStatusSerializer =
        new SystemStatusRequestSerializer();
    static final Serializer listTablesSerializer =
        new ListTablesRequestSerializer();
    static final Serializer getIndexesSerializer =
        new GetIndexesRequestSerializer();
    static final Serializer writeMultipleSerializer =
        new WriteMultipleRequestSerializer();
    static final Serializer multiDeleteSerializer =
        new MultiDeleteRequestSerializer();
    static final Serializer addReplicaSerializer =
        new AddReplicaRequestSerializer();
    static final Serializer dropReplicaSerializer =
        new DropReplicaRequestSerializer();
    static final Serializer getReplicaStatsSerializer =
        new GetReplicaStatsRequestSerializer();

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

    @Override
    public Serializer createAddReplicaSerializer() {
        return addReplicaSerializer;
    }

    @Override
    public Serializer createDropReplicaSerializer() {
        return dropReplicaSerializer;
    }

    @Override
    public Serializer createGetReplicaStatsSerializer() {
        return getReplicaStatsSerializer;
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

    @Override
    public Serializer createAddReplicaDeserializer() {
        return addReplicaSerializer;
    }

    @Override
    public Serializer createDropReplicaDeserializer() {
        return dropReplicaSerializer;
    }
    @Override
    public Serializer createGetReplicaStatsDeserializer() {
        return getReplicaStatsSerializer;
    }

    @Override
    public String getSerdeVersionString() {
        /* TODO: do we need this yet? */
        return "v4";
    }

    @Override
    public void writeSerialVersion(short serialVersion, ByteOutputStream bos)
        throws IOException {
        bos.writeShort(serialVersion);
    }

    /* serializers */

    /**
     * Table request:
     *  Payload:
     *    table name (if needed)
     *    statement (DDL)
     *    limits (if required -- create/alter)
     *    tags
     *    etag (string)
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
            writeTags(ns, rq);
            if (rq.getMatchETag() != null) {
                writeMapField(ns, ETAG, rq.getMatchETag());
            }
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
            writeConsistency(ns, rq.getConsistencyInternal());
            /* writeKey uses the output stream directly */
            writeKey(ns, rq);
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            GetResult result = new GetResult();

            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(ROW)) {
                    readRow(in, result);
                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, result);
                } else {
                    skipUnknownField(walker, name);
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
            serializeInternal(rq, ns);

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            DeleteResult result = new DeleteResult();

            in.setOffset(0);
            MapWalker walker = getMapWalker(in);
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
                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, result);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
        }

        /* shared with WriteMultiple */
        void serializeInternal(DeleteRequest rq, NsonSerializer ns)
            throws IOException {

            if (rq.getMatchVersion() != null) {
                writeMapField(ns, ROW_VERSION, rq.getMatchVersion().getBytes());
            }
            if (rq.getRowMetadata() != null) {
                writeMapField(ns, ROW_METADATA, rq.getRowMetadata());
            }


            /* writeValue uses the output stream directly */
            writeKey(ns, rq);
        }
    }

    /**
     * MultiDelete request:
     *  Payload:
     *    table name
     *    durability
     *    key
     *    range
     *    maxWriteKB
     *    continuation key
     *
     * MultiDelete result:
     *  consumed capacity
     *  numDeletions
     *  continuation key
     */
    public static class MultiDeleteRequestSerializer
        extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            MultiDeleteRequest rq = (MultiDeleteRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.MULTI_DELETE.ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, DURABILITY,
                          getDurability(rq.getDurability()));
            writeMapField(ns, MAX_WRITE_KB, rq.getMaxWriteKB());
            writeContinuationKey(ns, rq.getContinuationKey());
            writeFieldRange(ns, rq.getRange());
            writeKey(ns, rq);
            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            MultiDeleteResult result = new MultiDeleteResult();

            in.setOffset(0);
            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, result);
                } else if (name.equals(NUM_DELETIONS)) {
                    result.setNumDeletions(Nson.readNsonInt(in));
                } else if (name.equals(CONTINUATION_KEY)) {
                    result.setContinuationKey(Nson.readNsonBinary(in));
                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, result);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
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
            serializeInternal(rq, ns);

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            PutResult result = new PutResult();

            in.setOffset(0);
            MapWalker walker = getMapWalker(in);
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
                    result.setGeneratedValue(Nson.readFieldValue(in));
                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, result);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
        }

        /**
         * An internal method shared with WriteMultiple to serialize the
         * shared parts of the request
         * @param rq the request
         * @param ns the serializer
         * @throws IOException
         */
        public void serializeInternal(PutRequest rq,
                                      NsonSerializer ns)
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
            if (rq.getRowMetadata() != null) {
                writeMapField(ns, ROW_METADATA, rq.getRowMetadata());
            }

            /* writeValue uses the output stream directly */
            writeValue(ns, rq.getValue());
        }
    }


    /**
     * Query request:
     *  Payload:
     *    table name
     *    consistency
     *    key (an NSON object)
     *
     * Query result (all optional):
     *  consumed capacity
     *  meta: mod time, expiration, version
     *  value
     */
    public static class QueryRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {
                throw new IllegalArgumentException("Missing query version " +
                          "in query request serializer");
        }

        @SuppressWarnings("deprecation")
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              short queryVersion,
                              ByteOutputStream out)
            throws IOException {

            QueryRequest rq = (QueryRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.QUERY.ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);

            writeConsistency(ns, rq.getConsistency());
            if (rq.getDurability() != null) {
                writeMapField(ns, DURABILITY,
                              getDurability(rq.getDurability()));
            }

            /* these are only written if nonzero */
            writeMapFieldNZ(ns, MAX_READ_KB, rq.getMaxReadKB());
            writeMapFieldNZ(ns, MAX_WRITE_KB, rq.getMaxWriteKB());
            writeMapFieldNZ(ns, NUMBER_LIMIT, rq.getLimit());
            writeMapFieldNZ(ns, TRACE_LEVEL, rq.getTraceLevel());
            if (rq.getTraceLevel() > 0) {
                writeMapField(ns, TRACE_AT_LOG_FILES, rq.getLogFileTracing());
                writeMapField(ns, BATCH_COUNTER, rq.getBatchCounter());
            }

            writeMapField(ns, QUERY_VERSION, (int)queryVersion);
            boolean isPrepared = rq.isPrepared();
            if (isPrepared) {
                writeMapField(ns, IS_PREPARED, isPrepared);
                writeMapField(ns, IS_SIMPLE_QUERY, rq.isSimpleQuery());
                writeMapField(ns, PREPARED_QUERY,
                              rq.getPreparedStatement().getStatement());
                /*
                 * validation of parallel ops is handled in
                 * QueryRequest.validate
                 */
                if (rq.getNumberOfOperations() != 0) {
                    writeMapField(ns, NUM_QUERY_OPERATIONS,
                                  rq.getNumberOfOperations());
                    writeMapField(ns, QUERY_OPERATION_NUM,
                                  rq.getOperationNumber());
                }
                writeBindVariables(ns, out,
                              rq.getPreparedStatement().getVariables());
            } else {
                writeMapField(ns, STATEMENT, rq.getStatement());
            }
            if (rq.getContinuationKey() != null) {
                writeMapField(ns, CONTINUATION_KEY, rq.getContinuationKey());
            }

            writeLongMapFieldNZ(ns, SERVER_MEMORY_CONSUMPTION,
                                rq.getMaxServerMemoryConsumption());
            writeMathContext(ns, rq.getMathContext());

            if (rq.getShardId() != -1) { // default
                writeMapField(ns, SHARD_ID, rq.getShardId());
            }

            if (queryVersion >= QueryDriver.QUERY_V4) {
                if (rq.getQueryName() != null) {
                    writeMapField(ns, QUERY_NAME, rq.getQueryName());
                }
                if (rq.getVirtualScan() != null) {
                    writeVirtualScan(ns, rq.getVirtualScan(), queryVersion);
                }
                if (rq.getRowMetadata() != null) {
                    writeMapField(ns, ROW_METADATA, rq.getRowMetadata());
                }
            }

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        private static void writeVirtualScan(NsonSerializer ns,
                                             VirtualScan vs,
                                             short queryVersion)
            throws IOException {

            startMap(ns, VIRTUAL_SCAN);
            writeMapField(ns, VIRTUAL_SCAN_SID, vs.sid());
            writeMapField(ns, VIRTUAL_SCAN_PID, vs.pid());

            if (vs.isFirstBatch()) {
                int numTables = 1;
                if (queryVersion >= QueryDriver.QUERY_V5) {
                    numTables = vs.numTables();
                    writeMapField(ns, VIRTUAL_SCAN_NUM_TABLES, numTables);
                }
                for (int t = 0; t < numTables; ++t) {
                    writeMapField(ns, VIRTUAL_SCAN_CURRENT_INDEX_RANGE,
                                  vs.currentIndexRange(t));
                    writeMapField(ns, VIRTUAL_SCAN_PRIM_KEY,
                                  vs.primKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_SEC_KEY,
                                  vs.secKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_MOVE_AFTER,
                                  vs.moveAfterResumeKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY,
                                  vs.descResumeKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_TABLES,
                                  vs.joinPathTables(t));
                    writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_KEY,
                                  vs.joinPathKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_SEC_KEY,
                                  vs.joinPathSecKey(t));
                    writeMapField(ns, VIRTUAL_SCAN_JOIN_PATH_MATCHED,
                                  vs.joinPathMatched(t));
                }
            }

            endMap(ns, VIRTUAL_SCAN);
        }

        private static class DriverPlanInfo {
            PlanIter driverQueryPlan;
            int numIterators;
            int numRegisters;
            Map<String, Integer> externalVars;
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            throw new IllegalArgumentException("Missing query version " +
                      "in query request deserializer");
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion,
                                  short queryVersion) throws IOException {

            QueryRequest qreq = (QueryRequest) request;
            QueryResult result = new QueryResult(qreq);

            deserializePrepareOrQuery(qreq, result, null, null,
                                      in, serialVersion, queryVersion);
            return result;
        }


        /*
         * Deserialize either a QueryResult or a PrepareResult.
         * Either qreq/qres are given, or preq/pres are given.
         */
        private static void deserializePrepareOrQuery(
            QueryRequest qreq,
            QueryResult qres,
            PrepareRequest preq,
            PrepareResult pres,
            ByteInputStream in,
            short serialVersion,
            short queryVersion) throws IOException {

            PreparedStatement prep = null;
            if (qreq != null ) {
                prep = qreq.getPreparedStatement();
            }
            boolean isPreparedRequest = (prep != null);

            byte[] proxyPreparedQuery = null;

            DriverPlanInfo dpi = null;

            String queryPlan = null;
            String tableName = null;
            String namespace = null;
            String querySchema = null;
            byte operation = 0;
            int proxyTopoSeqNum = -1; /* QUERY_V3 and earlier */
            int[] shardIds = null; /* QUERY_V3 and earlier */
            byte[] contKey = null;
            VirtualScan[] virtualScans = null;
            TreeMap<String, String> queryTraces = null;
            int maxParallelism = 0; /* default value */

            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();

                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);

                } else if (name.equals(CONSUMED)) {
                    readConsumedCapacity(in, (qres != null ? qres : pres));

                } else if (name.equals(QUERY_RESULTS) && qres != null) {
                    qres.setResults(readQueryResults(in));

                } else if (name.equals(CONTINUATION_KEY)) {
                    contKey = Nson.readNsonBinary(in);

                } else if (name.equals(SORT_PHASE1_RESULTS) && qres != null) {
                    byte[] arr = Nson.readNsonBinary(in);
                    readPhase1Results(arr, qres);

                } else if (name.equals(PREPARED_QUERY)) {
                    proxyPreparedQuery = Nson.readNsonBinary(in);

                } else if (name.equals(DRIVER_QUERY_PLAN)) {
                    dpi = getDriverPlanInfo(Nson.readNsonBinary(in),
                                            serialVersion);

                } else if (name.equals(REACHED_LIMIT) && qres != null) {
                    qres.setReachedLimit(Nson.readNsonBoolean(in));

                } else if (name.equals(TABLE_NAME)) {
                    tableName = Nson.readNsonString(in);

                } else if (name.equals(NAMESPACE)) {
                    namespace = Nson.readNsonString(in);

                } else if (name.equals(QUERY_PLAN_STRING)) {
                    queryPlan = Nson.readNsonString(in);

                } else if (name.equals(QUERY_RESULT_SCHEMA)) {
                    querySchema = Nson.readNsonString(in);

                } else if (name.equals(QUERY_OPERATION)) {
                    operation = (byte)Nson.readNsonInt(in);

                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, (qres != null ? qres : pres));

                /* QUERY_V3 and earlier return topo differently */
                } else if (name.equals(PROXY_TOPO_SEQNUM)) {
                    proxyTopoSeqNum = Nson.readNsonInt(in);

                } else if (name.equals(SHARD_IDS)) {
                    shardIds = readNsonIntArray(in);

                /* added in QUERY_V4 */
                } else if (name.equals(VIRTUAL_SCANS)) {
                    readType(in, Nson.TYPE_ARRAY);
                    in.readInt(); /* length of array in bytes */
                    int numScans = in.readInt(); /* number of array elements */
                    virtualScans = new VirtualScan[numScans];
                    for (int i = 0; i < numScans; ++i) {
                        virtualScans[i] = readVirtualScan(in);
                    }

                /* added in QUERY_V4 */
                } else if (name.equals(QUERY_BATCH_TRACES)) {
                    readType(in, Nson.TYPE_ARRAY);
                    in.readInt(); /* length of array in bytes */
                    int numTraces = in.readInt() / 2; /* number of array elements */
                    queryTraces = new TreeMap<String,String>();
                    for (int i = 0; i < numTraces; ++i) {
                        String batchName = Nson.readNsonString(in);
                        String batchTrace = Nson.readNsonString(in);
                        queryTraces.put(batchName, batchTrace);
                    }
                } else if (name.equals(MAX_QUERY_PARALLELISM)) {
                    maxParallelism = Nson.readNsonInt(in);
                } else {
                    // log/warn
                    walker.skip();
                }
            }

            /* QUERY_V3 and earlier return topo differently */
            Result res = (qres != null) ? qres : pres;
            if (res.getTopology() == null && proxyTopoSeqNum >= 0) {
                res.setTopology(new TopologyInfo(proxyTopoSeqNum, shardIds));
            }

            if (qres != null) {
                qres.setContinuationKey(contKey);
                qreq.setContKey(qres.getContinuationKey());
                qres.setVirtualScans(virtualScans);
                qres.setQueryTraces(queryTraces);
            }

            if (isPreparedRequest) {
                return;
            }

            String statement;
            if (qreq != null) {
                statement = qreq.getStatement();
            } else {
                statement = preq.getStatement();
            }

            prep = new PreparedStatement(statement,
                                         queryPlan,
                                         querySchema,
                                         proxyPreparedQuery,
                                         (dpi!=null)?dpi.driverQueryPlan:null,
                                         (dpi!=null)?dpi.numIterators:0,
                                         (dpi!=null)?dpi.numRegisters:0,
                                         (dpi!=null)?dpi.externalVars:null,
                                         namespace,
                                         tableName,
                                         operation,
                                         maxParallelism);
            if (pres != null) {
                pres.setPreparedStatement(prep);
            } else if (qreq != null) {
                qreq.setPreparedStatement(prep);
                if (!prep.isSimpleQuery()) {
                    QueryDriver driver = new QueryDriver(qreq);
                    driver.setPrepCost(qres.getReadKB());
                    qres.setComputed(false);
                }
            }
        }

        private static VirtualScan readVirtualScan(ByteInputStream in)
            throws IOException {

            int sid = -1;
            int pid = -1;
            byte[] primKey = null;
            byte[] secKey = null;
            boolean moveAfter = true;
            byte[] descResumeKey = null;
            int[] joinPathTables = null;
            byte[] joinPathKey = null;
            byte[] joinPathSecKey = null;
            boolean joinPathMatched = false;
            int currentIndexRange = 0;
            int numTables = 1;
            int currTable = 0;
            TableResumeInfo[] tableRIs = new TableResumeInfo[1];

            MapWalker walker = getMapWalker(in);

            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(VIRTUAL_SCAN_SID)) {
                    sid = Nson.readNsonInt(in);
                } else if (name.equals(VIRTUAL_SCAN_PID)) {
                    pid = Nson.readNsonInt(in);
                } else if (name.equals(VIRTUAL_SCAN_NUM_TABLES)) {
                    numTables = Nson.readNsonInt(in);
                    tableRIs = new TableResumeInfo[numTables];
                } else if (name.equals(VIRTUAL_SCAN_CURRENT_INDEX_RANGE)) {
                    currentIndexRange = Nson.readNsonInt(in);
                } else if (name.equals(VIRTUAL_SCAN_PRIM_KEY)) {
                    primKey = Nson.readNsonBinary(in);
                } else if (name.equals(VIRTUAL_SCAN_SEC_KEY)) {
                    secKey = Nson.readNsonBinary(in);
                } else if (name.equals(VIRTUAL_SCAN_MOVE_AFTER)) {
                    moveAfter = Nson.readNsonBoolean(in);
                } else if (name.equals(VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY)) {
                    descResumeKey = Nson.readNsonBinary(in);
                } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_TABLES)) {
                    joinPathTables = Nson.readIntArray(in);
                } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_KEY)) {
                    joinPathKey = Nson.readNsonBinary(in);
                } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_SEC_KEY)) {
                    joinPathSecKey = Nson.readNsonBinary(in);
                } else if (name.equals(VIRTUAL_SCAN_JOIN_PATH_MATCHED)) {
                    joinPathMatched = Nson.readNsonBoolean(in);
                    tableRIs[currTable] = new TableResumeInfo(currentIndexRange,
                                                              primKey,
                                                              secKey,
                                                              moveAfter,
                                                              descResumeKey,
                                                              joinPathTables,
                                                              joinPathKey,
                                                              joinPathSecKey,
                                                              joinPathMatched);
                    ++currTable;
                } else {
                    skipUnknownField(walker, name);
                }
            }

            return new VirtualScan(pid, sid, tableRIs);
        }

        private static void readPhase1Results(byte[] arr, QueryResult result)
            throws IOException {
            ByteBuf buf = Unpooled.wrappedBuffer(arr);
            ByteInputStream bis = new NettyByteInputStream(buf);
            result.setIsInPhase1(bis.readBoolean());
            int[] pids = Nson.readIntArray(bis);
            if (pids != null) {
                result.setPids(pids);
                result.setNumResultsPerPid(Nson.readIntArray(bis));
                byte[][] contKeys = new byte[pids.length][];
                for (int i = 0; i < pids.length; ++i) {
                    contKeys[i] = Nson.readByteArray(bis);
                }
                result.setPartitionContKeys(contKeys);
            }
        }

        private static DriverPlanInfo getDriverPlanInfo(byte[] arr,
                                                        short serialVersion)
            throws IOException {
            if (arr == null || arr.length == 0) {
                return null;
            }
            ByteBuf buf = Unpooled.wrappedBuffer(arr);
            ByteInputStream bis = new NettyByteInputStream(buf);
            DriverPlanInfo dpi = new DriverPlanInfo();
            dpi.driverQueryPlan = PlanIter.deserializeIter(bis, serialVersion);
            if (dpi.driverQueryPlan == null) {
                return null;
            }
            dpi.numIterators = bis.readInt();
            dpi.numRegisters = bis.readInt();
            int len = bis.readInt();
            if (len <= 0) {
                return dpi;
            }
            dpi.externalVars = new HashMap<String, Integer>(len);
            for (int i = 0; i < len; ++i) {
                String varName = Nson.readString(bis);
                int varId = bis.readInt();
                dpi.externalVars.put(varName, varId);
            }
            return dpi;
        }

        private static List<MapValue> readQueryResults(ByteInputStream bis)
            throws IOException {
            int t = bis.readByte();
            if (t != Nson.TYPE_ARRAY) {
                throw new IllegalArgumentException("Bad type in queryResults: "+
                            Nson.typeString(t) + ", should be ARRAY");
            }
            bis.readInt(); /* length of array in bytes */
            int numElements = bis.readInt(); /* number of array elements */
            List<MapValue> results = new ArrayList<MapValue>(numElements);
            for (int i = 0; i < numElements; i++) {
                 results.add(Nson.readNsonMap(bis));
            }
            return results;
        }

        /*
         * Bind variables:
         * "variables": [
         *   { "name": "foo", "value": {...}},
         *   .....
         * ]
         */
        private void writeBindVariables(NsonSerializer ns,
                                        ByteOutputStream bos,
                                        Map<String, FieldValue> vars)
            throws IOException {
            if (vars == null || vars.size() == 0) {
                return;
            }
            startArray(ns, BIND_VARIABLES);
            for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
                ns.startMap(0);
                writeMapField(ns, NAME, entry.getKey());
                ns.startMapField(VALUE);
                Nson.writeFieldValue(bos, entry.getValue());
                ns.endMapField(VALUE);
                ns.endMap(0);
                ns.incrSize(1);
            }
            endArray(ns, BIND_VARIABLES);
        }
    }

    public static class PrepareRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {
                throw new IllegalArgumentException("Missing query version " +
                          "in prepare request serializer");
            }

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              short queryVersion,
                              ByteOutputStream out)
            throws IOException {

            PrepareRequest rq = (PrepareRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.PREPARE.ordinal(), rq);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);

            writeMapField(ns, QUERY_VERSION, (int)queryVersion);
            writeMapField(ns, STATEMENT, rq.getStatement());
            if (rq.getQueryPlan()) {
                writeMapField(ns, GET_QUERY_PLAN, true);
            }
            if (rq.getQuerySchema()) {
                writeMapField(ns, GET_QUERY_SCHEMA, true);
            }

            endMap(ns, PAYLOAD);
            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            throw new IllegalArgumentException("Missing query version " +
                      "in prepare request deserializer");
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion,
                                  short queryVersion) throws IOException {
            PrepareRequest prepRq = (PrepareRequest) request;
            PrepareResult result = new PrepareResult();

            QueryRequestSerializer.deserializePrepareOrQuery(
                                   null, null, prepRq, result,
                                   in, serialVersion, queryVersion);
            return result;
        }
    }

    /**
     * WriteMultiple request:
     *  Payload:
     *   table name: if all requests use same table
     *   durability
     *   operations array:
     *    for each delete/write:
     *      tablename, if using many tables
     *      opcode
     *      abortIfUnsuccessful boolean
     *      the delete or write payload, without durability
/     *
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
            writeMapField(ns, VERSION, V4_VERSION);
            /*
             * TableName
             * If all ops use the same table name, write that
             * single table name to the output stream.
             * If any of them are different, write all table
             * names to the individual ops.
             * Possible optimization: if most use one table,
             * write that in the header and only write minority
             * table names in specific ops.
             */
            if (rq.isSingleTable()) {
                writeMapField(ns, TABLE_NAME, rq.getTableName());
            }
            writeMapField(ns, OP_CODE, OpCode.WRITE_MULTIPLE.ordinal());
            writeMapField(ns, TIMEOUT, rq.getTimeoutInternal());
            endMap(ns, HEADER);

            /*
             * payload
             *
             * IMPORTANT: durability MUST be ordered
             * ahead of the operations or the server can't easily
             * deserialize efficiently
             */
            startMap(ns, PAYLOAD);
            writeMapField(ns, DURABILITY,
                          getDurability(rq.getDurability()));
            writeMapField(ns, NUM_OPERATIONS,
                          rq.getOperations().size());

            startArray(ns, OPERATIONS);
            for (OperationRequest op : rq.getOperations()) {
                /*
                 * Each operation is a map in the array.
                 * Calling the generic put or delete serializer will add
                 * redundant, unnecessary state. In order to share code
                 * with those serializers they have internal methods that
                 * write just what WriteMultiple requires. The exception
                 * is the op code and return row information, so write
                 * that here, then call the shared methods.
                 */
                ns.startMap(0);
                WriteRequest wr = op.getRequest();
                /*
                 * write op first -- this is important!
                 */
                if (wr instanceof PutRequest) {
                    PutRequest prq = (PutRequest) wr;
                    if (!rq.isSingleTable()) {
                        writeMapField(ns, TABLE_NAME, prq.getTableName());
                    }
                    writeMapField(ns, OP_CODE, getOpCode(prq).ordinal());
                    ((PutRequestSerializer)putSerializer).
                        serializeInternal(prq, ns);
                } else {
                    DeleteRequest drq = (DeleteRequest) wr;
                    if (!rq.isSingleTable()) {
                        writeMapField(ns, TABLE_NAME, drq.getTableName());
                    }
                    OpCode opCode = drq.getMatchVersion() != null ?
                        OpCode.DELETE_IF_VERSION : OpCode.DELETE;
                    writeMapField(ns, OP_CODE, opCode.ordinal());
                    ((DeleteRequestSerializer)delSerializer).
                        serializeInternal(drq, ns);
                }
                /* common to both delete and put */
                writeMapField(ns, RETURN_ROW, wr.getReturnRowInternal());
                if (op.isAbortIfUnsuccessful()) {
                    writeMapField(ns, ABORT_ON_FAIL,
                                  op.isAbortIfUnsuccessful());
                }
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
            MapWalker walker = getMapWalker(in);
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
                    in.readInt();
                    int numElements = in.readInt();
                    for (int i = 0; i < numElements; i++) {
                        result.addResult(createOperationResult(in));
                    }
                } else if (name.equals(WM_FAILURE)) {
                    /* failure is a map */
                    MapWalker fw = new MapWalker(in);
                    while (fw.hasNext()) {
                        fw.next();
                        String fname = fw.getCurrentName();
                        if (fname.equals(WM_FAIL_INDEX)) {
                            result.setFailedOperationIndex(
                                Nson.readNsonInt(in));
                        } else if (fname.equals(WM_FAIL_RESULT)) {
                            result.addResult(createOperationResult(in));
                        } else {
                            skipUnknownField(fw, name);
                        }
                    }
                } else if (name.equals(TOPOLOGY_INFO)) {
                    readTopologyInfo(in, result);
                } else {
                    skipUnknownField(walker, name);
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

            MapWalker walker = new MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(SUCCESS)) {
                    opResult.setSuccess(Nson.readNsonBoolean(in));
                } else if (name.equals(ROW_VERSION)) {
                    opResult.setVersion(Version.createVersion(
                                            Nson.readNsonBinary(in)));
                } else if (name.equals(GENERATED)) {
                    opResult.setGeneratedValue(Nson.readFieldValue(in));
                } else if (name.equals(RETURN_INFO)) {
                    readReturnInfo(in, opResult);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return opResult;
        }
    }

    /**
     * System request:
     *  Payload:
     *    statement (DDL)
     *
     * System result:
     *  state (int)
     *  operation id (string)
     *  statement(string)
     *  result string (string)
     */
    public static class SystemRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            SystemRequest rq = (SystemRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.SYSTEM_REQUEST.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, STATEMENT,
                          Nson.getCharArrayAsUTF8(rq.getStatement()));
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            return deserializeSystemResult(request, in);
        }
    }

    /**
     * System status request:
     *  Payload:
     *    statement (DDL)
     *    operation id
     *
     * System result:
     *  state (int)
     *  operation id (string)
     *  statement(string)
     *  result string (string)
     */
    public static class SystemStatusRequestSerializer
        extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            SystemStatusRequest rq = (SystemStatusRequest) request;

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0);

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.SYSTEM_STATUS_REQUEST.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, OPERATION_ID, rq.getOperationId());
            writeMapField(ns, STATEMENT, rq.getStatement());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            return deserializeSystemResult(request, in);
        }
    }

    /**
     * List tables request:
     *  Payload:
     *    start index (int)
     *    limit (int)
     *    namespace (string, on-prem only)
     *
     * List tables result:
     *  array of table names (string)
     *  last index returned (int)
     *
     *  if no tables, the result is empty and this code should create an
     *  empty array for the API
     */
    public static class ListTablesRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            ListTablesRequest rq = (ListTablesRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.LIST_TABLES.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, LIST_START_INDEX, rq.getStartIndex());
            writeMapField(ns, LIST_MAX_TO_READ, rq.getLimit());
            writeMapField(ns, NAMESPACE, rq.getNamespace());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            ListTablesResult  result = new ListTablesResult();
            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(TABLES)) {
                    /* array of table names */
                    int t = in.readByte();
                    if (t != Nson.TYPE_ARRAY) {
                        throw new IllegalStateException(
                            "Operations: bad type in list tables result: " +
                            Nson.typeString(t) + ", should be ARRAY");
                    }
                    in.readInt();
                    int numElements = in.readInt();
                    String[] tables = new String[numElements];
                    for (int i = 0; i < numElements; i++) {
                        tables[i] = Nson.readNsonString(in);
                    }
                    result.setTables(tables);
                } else if (name.equals(LAST_INDEX)) {
                    result.setLastIndexReturned(Nson.readNsonInt(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
            /*
             * The result API guarantees non-null, even if empty
             */
            if (result.getTables() == null) {
                result.setTables(new String[0]);
            }
            return result;
        }
    }

    /**
     * Get indexes request:
     *  Payload:
     *    tableName (in header)
     *    index name (optional -- if no set, get all indexes on table)
     *
     * Get indexes result:
     *  array of indexes, where each is:
     *  "index": {
     *    "name": ...
     *    "fields": [field1, ..., fieldN]
     *  }
     *
     *  if no index, the result is empty and this code should create an
     *  empty array for the API
     */
    public static class GetIndexesRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            GetIndexesRequest rq = (GetIndexesRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.GET_INDEXES.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, INDEX, rq.getIndexName());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            GetIndexesResult  result = new GetIndexesResult();

            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(INDEXES)) {
                    /* array of index info */
                    int t = in.readByte();
                    if (t != Nson.TYPE_ARRAY) {
                        throw new IllegalStateException(
                            "Operations: bad type in get indexes result: " +
                            Nson.typeString(t) + ", should be ARRAY");
                    }
                    in.readInt();
                    int numElements = in.readInt();
                    IndexInfo[] indexes = new IndexInfo[numElements];
                    for (int i = 0; i < numElements; i++) {
                        indexes[i] = readIndexInfo(in);
                    }
                    result.setIndexes(indexes);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            /*
             * The result API guarantees non-null, even if empty
             */
            if (result.getIndexes() == null) {
                result.setIndexes(new IndexInfo[0]);
            }
            return result;
        }

        private IndexInfo readIndexInfo(ByteInputStream in)
            throws IOException {
            MapWalker walker = new MapWalker(in);
            String indexName = null;
            String[] fields = null;
            String[] types = null;
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(NAME)) {
                    indexName = Nson.readNsonString(in);
                } else if (name.equals(FIELDS)) {
                    /* array of string */
                    int t = in.readByte();
                    if (t != Nson.TYPE_ARRAY) {
                        throw new IllegalStateException(
                            "Operations: bad type in get indexes result: " +
                            Nson.typeString(t) + ", should be ARRAY");
                    }
                    in.readInt();
                    int numElements = in.readInt();
                    fields = new String[numElements];
                    types = new String[numElements];
                    /* it's an array of map with PATH, TYPE elements */
                    for (int i = 0; i < numElements; i++) {
                        MapWalker infoWalker =
                            new MapWalker(in);
                        while (infoWalker.hasNext()) {
                            infoWalker.next();
                            String fname = infoWalker.getCurrentName();
                            if (fname.equals(PATH)) {
                                fields[i] = Nson.readNsonString(in);
                            } else if (fname.equals(TYPE)) {
                                types[i] = Nson.readNsonString(in);
                            } else {
                                skipUnknownField(infoWalker, fname);
                            }
                        }
                        if (fields[i] == null) {
                            throw new IllegalStateException(
                                "Bad GetIndexes result, missing path");
                        }
                    }
                } else {
                    skipUnknownField(walker, name);
                }
            }
            if (indexName == null || fields == null) {
                throw new IllegalStateException(
                    "Bad GetIndexes result, missing name or fields");
            }
            return new IndexInfo(indexName, fields, types);
        }
    }


    /**
     * Table Usage request:
     *  Payload:
     *    tableName (in header)
     *    start time (string)
     *    end time (string)
     *    limit (int)
     *
     * Table Usage result:
     *  table name (it's in the result class)
     *  array of TableUsage records:
     *  {
     *    "start_time": (string or long?)
     *    "seconds" : (int, seconds in period sample)
     *    "read_units" :
     *    "write_units" :
     *    "storage_gb" :
     *    "read_throttle_count":
     *    "write_throttle_count":
     *    "storage_throttle_count":
     *  }
     */
    public static class TableUsageRequestSerializer extends NsonSerializerBase {
        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            TableUsageRequest rq = (TableUsageRequest) request;

            NsonSerializer ns = new NsonSerializer(out);
            ns.startMap(0);

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.GET_TABLE_USAGE.ordinal(), request);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, START, rq.getStartTimeString());
            writeMapField(ns, END, rq.getEndTimeString());
            writeMapField(ns, LIST_MAX_TO_READ, rq.getLimit());
            writeMapField(ns, LIST_START_INDEX, rq.getStartIndex());

            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion) throws IOException {
            TableUsageResult result = new TableUsageResult();

            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(TABLE_NAME)) {
                    result.setTableName(Nson.readNsonString(in));
                } else if (name.equals(TABLE_USAGE)) {
                    /* array usage records */
                    int t = in.readByte();
                    if (t != Nson.TYPE_ARRAY) {
                        throw new IllegalStateException(
                            "Operations: bad type in table usage result: " +
                            Nson.typeString(t) + ", should be ARRAY");
                    }
                    in.readInt();
                    int numElements = in.readInt();
                    TableUsage[] usageRecords = new TableUsage[numElements];
                    for (int i = 0; i < numElements; i++) {
                        usageRecords[i] = readUsageRecord(in);
                    }
                    result.setUsageRecords(usageRecords);
                } else if (name.equals(LAST_INDEX)) {
                    result.setLastIndexReturned(Nson.readNsonInt(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
            /*
             * The result API guarantees non-null, even if empty
             */
            if (result.getUsageRecords() == null) {
                result.setUsageRecords(new TableUsage[0]);
            }
            return result;
        }

        private TableUsage readUsageRecord(ByteInputStream in)
            throws IOException {
            MapWalker walker = new MapWalker(in);
            TableUsage usage = new TableUsage();
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(START)) {
                    usage.startTimeMillis = timeToLong(Nson.readNsonString(in));
                } else if (name.equals(TABLE_USAGE_PERIOD)) {
                    usage.secondsInPeriod = Nson.readNsonInt(in);
                } else if (name.equals(READ_UNITS)) {
                    usage.readUnits = Nson.readNsonInt(in);
                } else if (name.equals(WRITE_UNITS)) {
                    usage.writeUnits = Nson.readNsonInt(in);
                } else if (name.equals(STORAGE_GB)) {
                    usage.storageGB = Nson.readNsonInt(in);
                } else if (name.equals(READ_THROTTLE_COUNT)) {
                    usage.readThrottleCount = Nson.readNsonInt(in);
                } else if (name.equals(WRITE_THROTTLE_COUNT)) {
                    usage.writeThrottleCount = Nson.readNsonInt(in);
                } else if (name.equals(STORAGE_THROTTLE_COUNT)) {
                    usage.storageThrottleCount = Nson.readNsonInt(in);
                } else if (name.equals(MAX_SHARD_USAGE_PERCENT)) {
                    usage.maxShardUsagePercent = Nson.readNsonInt(in);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return usage;
        }
    }

    /**
     * Add Replica request:
     *   tableName (in header) -- required
     *   region (string) -- required
     *   readUnits (int)
     *   writeUnits (int)
     *   etag (string)
     *
     * Table result (all optional):
     *   compartment ocid (string)
     *   namespace (string)
     *   table ocid (string)
     *   table name (string)
     *   state (int)
     *   schema (string)
     *   ddl (string)
     *   throughput info (read/write/storage)
     *   operation id (plan id, etc) (int)
     *   free form tags (string)
     *   defined tags (string)
     *   etag (string)
     *   isFrozen (boolean)
     *   initialized (boolean)
     *   replicas (array(<replica>))
     *     <replica>:
     *       region (string)
     *       tableOcid (string)
     *       writeUnits (int)
     *       mode (int)
     *       tableState (int): same value as TableResult.tableState
     */
    public static class AddReplicaRequestSerializer extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            AddReplicaRequest req = (AddReplicaRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            // header
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.ADD_REPLICA.ordinal(), req);
            endMap(ns, HEADER);

            // payload
            startMap(ns, PAYLOAD);
            writeMapField(ns, REGION, req.getReplicaName());
            writeMapFieldNZ(ns, READ_UNITS, req.getReadUnits());
            writeMapFieldNZ(ns, WRITE_UNITS, req.getWriteUnits());
            if (req.getMatchETag() != null) {
                writeMapField(ns, ETAG, req.getMatchETag());
            }
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion)
            throws IOException {

            return deserializeTableResult(request, in);
        }
    }

    /**
     * Drop Replica request:
     *  Payload:
     *    tableName (in header) -- required
     *    region (string) -- required
     *    etag (string)
     *
     * Table result (all optional):
     *   compartment ocid (string)
     *   namespace (string)
     *   table ocid (string)
     *   table name (string)
     *   state (int)
     *   schema (string)
     *   ddl (string)
     *   throughput info (read/write/storage)
     *   operation id (plan id, etc) (int)
     *   free form tags (string)
     *   defined tags (string)
     *   etag (string)
     *   isFrozen (boolean)
     *   initialized (boolean)
     *   replicas (array(<replica>))
     *     <replica>:
     *       region (string)
     *       tableOcid (string)
     *       writeUnits (int)
     *       mode (int)
     *       tableState (int): same value as TableResult.tableState
     */
    public static class DropReplicaRequestSerializer extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            DropReplicaRequest req = (DropReplicaRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            /* header */
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.DROP_REPLICA.ordinal(), req);
            endMap(ns, HEADER);

            /* payload */
            startMap(ns, PAYLOAD);
            writeMapField(ns, REGION, req.getReplicaName());
            if (req.getMatchETag() != null) {
                writeMapField(ns, ETAG, req.getMatchETag());
            }
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion)
            throws IOException {

            return deserializeTableResult(request, in);
        }
    }

    /**
     * GetReplicaStatsRequest:
     *   tableName (in header)  -- required
     *   replica (string)  -- required
     *   startTime (string)
     *   limit (int)
     *
     * GetReplicaStatsResult:
     *   tableName (string)
     *   nextStartTime (long)
     *   replicaStats (Map<string, Array<ReplicaStats>>)
     *     key - region (string)
     *     value - Array<ReplicaStats>
     *
     *     ReplicaStats:
     *       time (long)
     *       replicaLag (int)
     */
    public static class GetReplicaStatsRequestSerializer
        extends NsonSerializerBase {

        @Override
        public void serialize(Request request,
                              short serialVersion,
                              ByteOutputStream out)
            throws IOException {

            ReplicaStatsRequest req = (ReplicaStatsRequest) request;

            /* use NsonSerializer and direct writing to serialize */

            NsonSerializer ns = new Nson.NsonSerializer(out);
            ns.startMap(0); // top-level object

            /* header */
            startMap(ns, HEADER);
            writeHeader(ns, OpCode.GET_REPLICA_STATS.ordinal(), req);
            endMap(ns, HEADER);

            /* payload */
            startMap(ns, PAYLOAD);
            writeMapField(ns, REGION, req.getReplicaName());
            writeMapField(ns, START, req.getStartTimeString());
            writeMapFieldNZ(ns, LIST_MAX_TO_READ, req.getLimit());
            endMap(ns, PAYLOAD);

            ns.endMap(0); // top level object
        }

        @Override
        public Result deserialize(Request request,
                                  ByteInputStream in,
                                  short serialVersion)
            throws IOException {

            ReplicaStatsResult result = new ReplicaStatsResult();

            in.setOffset(0);
            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(TABLE_NAME)) {
                    result.setTableName(Nson.readNsonString(in));
                } else if (name.equals(REPLICA_STATS)) {
                    readReplicasStatsRecord(in, result);
                } else if (name.equals(NEXT_START_TIME)) {
                    result.setNextStartTime(Nson.readNsonLong(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
        }

        /*
         * replicaStats (Map<string, Array<ReplicaStats>>)
         *   key - region (string)
         *   value - Array<ReplicaStats>
         *
         *   ReplicaStats :
         *     time (long)
         *     replicaLag (int)
         */
        private void readReplicasStatsRecord(ByteInputStream in,
                                             ReplicaStatsResult result)
            throws IOException {

            MapWalker walker = new MapWalker(in);
            Map<String, ReplicaStats[]> repRecords = new HashMap<>();
            while (walker.hasNext()) {
                walker.next();
                String replicaName = walker.getCurrentName();

                /* array of replica stats */
                int t = in.readByte();
                if (t != Nson.TYPE_ARRAY) {
                    throw new IllegalStateException(
                        "Operations: bad type in replica stats: " +
                        Nson.typeString(t) + ", should be ARRAY");
                }

                in.readInt();
                int numElements = in.readInt();
                ReplicaStats[] records = new ReplicaStats[numElements];

                for (int i = 0; i < numElements; i++) {
                    ReplicaStats stats = new ReplicaStats();
                    readReplicaStats(in, stats);
                    records[i] = stats;
                }

                repRecords.put(replicaName, records);
            }
            result.setStatsRecords(repRecords);
        }

        /*
         * ReplicaStats:
         *    time (long)
         *    replicaLag (int)
         */
        private void readReplicaStats(ByteInputStream in, ReplicaStats stats)
            throws IOException {

            MapWalker walker = new MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(TIME)) {
                    stats.collectionTimeMillis = Nson.readNsonLong(in);
                } else if (name.equals(REPLICA_LAG)) {
                    stats.replicaLag = Nson.readNsonInt(in);
                } else {
                    skipUnknownField(walker, name);
                }
            }
        }
    }

    /**
     * Base class that implements common methods for serialization and
     * deserialization of V4 protocol
     */
    public static abstract class NsonSerializerBase implements Serializer {

        public static void readType(ByteInputStream in, int expected)
            throws IOException {

            int type = in.readByte();
            if (type != expected) {
                throwTypeMismatch(expected, type);
            }
        }

        private static void throwTypeMismatch(int expected, int found) {
            throw new IllegalArgumentException(
               "Expected type not found, expected type: " +
               Nson.typeString(expected) +
               ", found type: " + Nson.typeString(found));
        }

        /**
         * Header
         *  version (int)
         *  operation (int)
         *  sequence number of cached topology, if available
         *  timeout (int)
         *  tableName if available
         *   it is helpful to have the tableName available as early as possible
         *   when processing requests as it's used for authorization, filtering,
         *   etc. It's not present in all requests and there may be a future
         *   where a request can have multiple tables but that information
         *   would be in the payload
         */
        protected static void writeHeader(NsonSerializer ns, int op, Request rq)
            throws IOException {

            writeMapField(ns, VERSION, V4_VERSION);
            if (rq.getTableName() != null) {
                writeMapField(ns, TABLE_NAME, rq.getTableName());
            }
            writeMapField(ns, OP_CODE, op);
            writeMapField(ns, TOPO_SEQ_NUM, rq.topoSeqNum());
            writeMapField(ns, TIMEOUT, rq.getTimeoutInternal());
            if (rq.getPreferThrottling()) {
                writeMapField(ns, PREFER_THROTTLING, true);
            }
            if (rq.getDRLOptIn()) {
                writeMapField(ns, DRL_OPTIN, true);
            }
        }

        /**
         * Writes consistency
         * "consistency": {
         *   "type": number
         * }
         * consistency (int)
         */
        protected static void writeConsistency(NsonSerializer ns,
                                               Consistency consistency)
            throws IOException {
            startMap(ns, CONSISTENCY);
            writeMapField(ns, TYPE, getConsistencyType(consistency));
            endMap(ns, CONSISTENCY);
        }

        /**
         * Writes common fields for write requests -- table name,
         * durability, return row
         */
        protected static void writeWriteRequest(NsonSerializer ns,
                                                WriteRequest rq)
            throws IOException {

            writeMapField(ns, DURABILITY,
                          getDurability(rq.getDurability()));
            writeMapField(ns, RETURN_ROW, rq.getReturnRowInternal());
        }

        /**
         * Writes a primary key:
         *  "key": {...}
         * The request may be a GetRequest, DeleteRequest, or
         * MultiDeleteRequest
         */
        protected static void writeKey(NsonSerializer ns, Request rq)
            throws IOException {

            MapValue key = (rq instanceof GetRequest ?
                            ((GetRequest)rq).getKey() :
                            (rq instanceof DeleteRequest ?
                             ((DeleteRequest)rq).getKey() :
                             ((MultiDeleteRequest)rq).getKey()));
            if (key == null) {
                throw new IllegalArgumentException("Key cannot be null");
            }

            ns.startMapField(KEY);
            Nson.writeFieldValue(ns.getStream(), key);
            ns.endMapField(KEY);
        }

        protected void writeMathContext(NsonSerializer ns,
                                        MathContext mathContext)
            throws IOException {

            int val = 0;
            if (mathContext == null) {
                return;
            } else if (MathContext.DECIMAL32.equals(mathContext)) {
                return; // default: no need to write
            } else if (MathContext.DECIMAL64.equals(mathContext)) {
                val = 2;
            } else if (MathContext.DECIMAL128.equals(mathContext)) {
                val = 3;
            } else if (MathContext.UNLIMITED.equals(mathContext)) {
                val = 4;
            } else {
                val = 5;
                writeMapField(ns, MATH_CONTEXT_PRECISION,
                              mathContext.getPrecision());
                writeMapField(ns, MATH_CONTEXT_ROUNDING_MODE,
                              mathContext.getRoundingMode().ordinal());

            }
            writeMapField(ns, MATH_CONTEXT_CODE, val);
        }

        /**
         * Writes a row value:
         *  "value": {...}
         */
        protected static void writeValue(NsonSerializer ns, FieldValue value)
            throws IOException {

            ns.startMapField(VALUE);
            Nson.writeFieldValue(ns.getStream(), value);
            ns.endMapField(VALUE);
        }

        protected static void writeMapField(NsonSerializer ns,
                                            String fieldName,
                                            int value) throws IOException {
            ns.startMapField(fieldName);
            ns.integerValue(value);
            ns.endMapField(fieldName);
        }

        /* only write field if value is nonzero */
        protected static void writeMapFieldNZ(NsonSerializer ns,
                                              String fieldName,
                                              int value) throws IOException {
            if (value != 0) {
                writeMapField(ns, fieldName, value);
            }
        }

        protected static void writeLongMapField(NsonSerializer ns,
                                                String fieldName,
                                                long value) throws IOException {
            ns.startMapField(fieldName);
            ns.longValue(value);
            ns.endMapField(fieldName);
        }

        protected static void writeLongMapFieldNZ(NsonSerializer ns,
                                                  String fieldName,
                                                  long value) throws IOException {
            if (value != 0) {
                writeLongMapField(ns, fieldName, value);
            }
        }

        protected static void writeMapField(NsonSerializer ns,
                                            String fieldName,
                                            String value) throws IOException {
            /* silently ignore null string */
            if (value != null) {
                ns.startMapField(fieldName);
                ns.stringValue(value);
                ns.endMapField(fieldName);
            }
        }

        protected static void writeMapField(NsonSerializer ns,
                                            String fieldName,
                                            boolean value) throws IOException {
            ns.startMapField(fieldName);
            ns.booleanValue(value);
            ns.endMapField(fieldName);
        }

        protected static void writeMapField(NsonSerializer ns,
                                            String fieldName,
                                            byte[] value) throws IOException {
            ns.startMapField(fieldName);
            ns.binaryValue(value);
            ns.endMapField(fieldName);
        }

        public static void writeMapField(NsonSerializer ns,
                                         String fieldName,
                                         int[] value) throws IOException {
            if (value == null || value.length == 0) {
                return;
            }
            ns.startMapField(fieldName);
            ns.startArray(0);
            for (int i : value) {
                ns.integerValue(i);
                ns.incrSize(1);
            }
            ns.endArray(0);
            ns.endMapField(fieldName);
        }

        protected static void startMap(NsonSerializer ns, String name)
            throws IOException {
            ns.startMapField(name);
            ns.startMap(0);
        }

        protected static void endMap(NsonSerializer ns, String name)
            throws IOException {
            ns.endMap(0);
            ns.endMapField(name);
        }

        protected static void startArray(NsonSerializer ns, String name)
            throws IOException {
            ns.startMapField(name);
            ns.startArray(0);
        }

        protected static void endArray(NsonSerializer ns, String name)
            throws IOException {
            ns.endArray(0);
            ns.endMapField(name);
        }

        protected static void writeLimits(NsonSerializer ns, TableLimits limits)
            throws IOException {
            if (limits != null) {
                startMap(ns, LIMITS);
                writeMapField(ns, READ_UNITS, limits.getReadUnits());
                writeMapField(ns, WRITE_UNITS, limits.getWriteUnits());
                writeMapField(ns, STORAGE_GB, limits.getStorageGB());
                TableLimits.CapacityMode mode = limits.getMode();
                int intMode = (mode == TableLimits.CapacityMode.PROVISIONED ?
                               PROVISIONED : ON_DEMAND);
                writeMapField(ns, LIMITS_MODE, intMode);
                endMap(ns, LIMITS);
            }
        }

        protected static void writeTags(NsonSerializer ns, TableRequest rq)
            throws IOException {
            DefinedTags dtags = rq.getDefinedTags();
            FreeFormTags ftags = rq.getFreeFormTags();
            if ( dtags != null) {
                writeMapField(ns, DEFINED_TAGS, dtags.toString());
            }
            if ( ftags != null) {
                writeMapField(ns, FREE_FORM_TAGS, ftags.toString());
            }
        }

        protected static int getConsistencyType(Consistency consistency) {
            if (consistency == null || consistency.isEventual()) {
                return EVENTUAL;
            } else if (consistency.isAbsolute()) {
                return ABSOLUTE;
            }
            throw new IllegalArgumentException("Unknown Consistency " +
                                               consistency);
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
        protected static int handleErrorCode(MapWalker walker)
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
                    skipUnknownField(walker, name);
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

            MapWalker walker = new MapWalker(in);
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
                    skipUnknownField(walker, name);
                }
            }
        }

        /*
         * "topology_info" : {
         *    "PROXY_TOPO_SEQNUM" : int
         *    "SHARD_IDS" : [ int, ... ]
         * }
         */
        static void readTopologyInfo(ByteInputStream in,
                                             Result result)
            throws IOException {

            int proxyTopoSeqNum = -1;
            int[] shardIds = null;
            MapWalker walker = new MapWalker(in);

            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(PROXY_TOPO_SEQNUM)) {
                    proxyTopoSeqNum = Nson.readNsonInt(in);
                } else if (name.equals(SHARD_IDS)) {
                    shardIds = readNsonIntArray(in);
                } else {
                    skipUnknownField(walker, name);
                }
            }

            TopologyInfo ti = null;
            if (proxyTopoSeqNum >= 0) {
                ti = new TopologyInfo(proxyTopoSeqNum, shardIds);
                result.setTopology(ti);
            }
        }

        // TODO: move this to Nson
        static int[] readNsonIntArray(ByteInputStream bis)
            throws IOException {
            int t = bis.readByte();
            if (t != Nson.TYPE_ARRAY) {
                throw new IllegalArgumentException(
                    "Bad type in integer array: "+
                    Nson.typeString(t) + ", should be ARRAY");
            }
            bis.readInt(); /* length of array in bytes */
            int numElements = bis.readInt(); /* number of array elements */
            int[] arr = new int[numElements];
            for (int i = 0; i < numElements; i++) {
                arr[i] = Nson.readNsonInt(bis);
            }
            return arr;
        }

        /**
         * Reads the row from a get operation which includes row metadata
         * and the value
         */
        static void readRow(ByteInputStream in, GetResult result)
            throws IOException {

            MapWalker walker = new MapWalker(in);
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
                } else if (name.equals(ROW_METADATA)) {
                    result.setRowMetadata(Nson.readNsonString(in));
                } else if (name.equals(VALUE)) {
                    result.setValue((MapValue)Nson.readFieldValue(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
        }

        /**
         * "return_info": {
         *    "existing_value": {}
         *    "existing_version": byte[]
         *    "existing_mod": long
         *    "existing_expiration": long
         *    "existing_row_metadata": String
         *  }
         *
         */
        static void readReturnInfo(ByteInputStream in,
                                   WriteResult result)
            throws IOException {

            MapWalker walker = new MapWalker(in);
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
                } else if (name.equals(EXISTING_ROW_METADATA)) {
                    result.setExistingRowMetadata(Nson.readNsonString(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
        }

        protected static void writeContinuationKey(NsonSerializer ns,
                                                   byte[] key)
            throws IOException {
            if (key != null) {
                writeMapField(ns, CONTINUATION_KEY, key);
            }
        }

        /*
         * "range": {
         *   "path": path to field (string)
         *   "start" {
         *      "value": {FieldValue}
         *      "inclusive": bool
         *   }
         *   "end" {
         *      "value": {FieldValue}
         *      "inclusive": bool
         *   }
         */
        protected static void writeFieldRange(NsonSerializer ns,
                                              FieldRange range)
            throws IOException {
            if (range != null) {
                startMap(ns, RANGE);
                writeMapField(ns, RANGE_PATH, range.getFieldPath());
                if (range.getStart() != null) {
                    startMap(ns, START);
                    writeValue(ns, range.getStart());
                    writeMapField(ns, INCLUSIVE,
                                  range.getStartInclusive());
                    endMap(ns, START);
                }
                if (range.getEnd() != null) {
                    startMap(ns, END);
                    writeValue(ns, range.getEnd());
                    writeMapField(ns, INCLUSIVE, range.getEndInclusive());
                    endMap(ns, END);
                }
                endMap(ns, RANGE);
            }
        }

        /*
         * Shared code to deserialize a SystemResult
         */
        protected static SystemResult deserializeSystemResult(
            Request request,
            ByteInputStream in) throws IOException {

            SystemResult result = new SystemResult();

            MapWalker walker = getMapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(ERROR_CODE)) {
                    handleErrorCode(walker);
                } else if (name.equals(SYSOP_STATE)) {
                    result.setState(getOperationState(Nson.readNsonInt(in)));
                } else if (name.equals(SYSOP_RESULT)) {
                    result.setResultString(Nson.readNsonString(in));
                } else if (name.equals(STATEMENT)) {
                    result.setStatement(Nson.readNsonString(in));
                } else if (name.equals(OPERATION_ID)) {
                    result.setOperationId(Nson.readNsonString(in));
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
        }

        private static SystemResult.State getOperationState(int state) {
            switch (state) {
            case COMPLETE:
                return SystemResult.State.COMPLETE;
            case WORKING:
                return SystemResult.State.WORKING;
            default:
                throw new IllegalStateException("Unknown operation state " +
                                                state);
            }
        }

        /*
         * Shared code to deserialize a TableResult
         */
        protected static TableResult deserializeTableResult(Request request,
                                                            ByteInputStream in)
            throws IOException {

            TableResult result = new TableResult();

            in.setOffset(0);

            MapWalker walker = getMapWalker(in);
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
                    result.setState(getTableState(Nson.readNsonInt(in)));
                } else if (name.equals(TABLE_SCHEMA)) {
                    result.setSchema(Nson.readNsonString(in));
                } else if (name.equals(TABLE_DDL)) {
                    result.setDdl(Nson.readNsonString(in));
                } else if (name.equals(OPERATION_ID)) {
                    result.setOperationId(Nson.readNsonString(in));
                } else if (name.equals(FREE_FORM_TAGS)) {
                    result.setFreeFormTags(
                        new FreeFormTags(Nson.readNsonString(in)));
                } else if (name.equals(DEFINED_TAGS)) {
                    result.setDefinedTags(
                        new DefinedTags(Nson.readNsonString(in)));
                } else if (name.equals(ETAG)) {
                    result.setMatchETag(Nson.readNsonString(in));
                } else if (name.equals(LIMITS)) {
                    MapWalker lw = new MapWalker(in);
                    int ru = 0;
                    int wu = 0;
                    int sg = 0;
                    int mode = PROVISIONED;
                    while (lw.hasNext()) {
                        lw.next();
                        name = lw.getCurrentName();
                        if (name.equals(READ_UNITS)) {
                            ru = Nson.readNsonInt(in);
                        } else if (name.equals(WRITE_UNITS)) {
                            wu = Nson.readNsonInt(in);
                        } else if (name.equals(STORAGE_GB)) {
                            sg = Nson.readNsonInt(in);
                        } else if (name.equals(LIMITS_MODE)) {
                            mode = Nson.readNsonInt(in);
                        } else {
                            skipUnknownField(lw, name);
                        }
                    }
                    result.setTableLimits(new TableLimits(
                                              ru, wu, sg,
                                              getCapacityMode(mode)));
                } else if (name.equals(SCHEMA_FROZEN)) {
                    result.setIsFrozen(Nson.readNsonBoolean(in));
                } else if (name.equals(INITIALIZED)) {
                    result.setLocalReplicaInitialized(Nson.readNsonBoolean(in));
                } else if (name.equals(REPLICAS)) {
                    readReplicas(in, result);
                } else {
                    skipUnknownField(walker, name);
                }
            }
            return result;
        }

        private static void readReplicas(ByteInputStream in, TableResult result)
            throws IOException {

            /* array of replicas */
            int t = in.readByte();
            if (t != Nson.TYPE_ARRAY) {
                throw new IllegalStateException(
                    "Replicas: bad type in table result: " +
                    Nson.typeString(t) + ", should be ARRAY");
            }
            in.readInt();
            int numElements = in.readInt();
            Replica[] replicas = new Replica[numElements];
            for (int i = 0; i < numElements; i++) {
                Replica replica = new Replica();
                readReplica(in, replica);
                replicas[i] = replica;
            }
            result.setReplicas(replicas);
        }

        private static void readReplica(ByteInputStream in, Replica replica)
            throws IOException {

            MapWalker walker = new MapWalker(in);
            while (walker.hasNext()) {
                walker.next();
                String name = walker.getCurrentName();
                if (name.equals(REGION)) {
                    replica.setReplicaName(Nson.readNsonString(in));
                } else if (name.equals(TABLE_OCID)) {
                    replica.setTableId(Nson.readNsonString(in));
                } else if (name.equals(WRITE_UNITS)) {
                    replica.setWriteUnits(Nson.readNsonInt(in));
                } else if (name.equals(LIMITS_MODE)) {
                    replica.setCapacityMode(
                        getCapacityMode(Nson.readNsonInt(in)));
                } else if (name.equals(TABLE_STATE)) {
                    replica.setState(getTableState(Nson.readNsonInt(in)));
                } else {
                    skipUnknownField(walker, name);
                }
            }
        }

        /*
         * From here down utilities to handle portions of requests or results
         */
        protected static OpCode getOpCode(PutRequest req) {
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

        protected static void skipUnknownField(MapWalker walker,
                                               String name)
            throws IOException {
            // TODO log/warn
            walker.skip();
        }

        protected static TableLimits.CapacityMode getCapacityMode(int mode) {
            switch (mode) {
            case PROVISIONED:
                return TableLimits.CapacityMode.PROVISIONED;
            case ON_DEMAND:
                return TableLimits.CapacityMode.ON_DEMAND;
            default:
                throw new IllegalStateException(
                    "Unknown capacity mode " + mode);
            }
        }

        protected static long timeToLong(String timestamp) {
            return new TimestampValue(timestamp).getLong();
        }

        /*
         * If the client is connected to a pre-V4 server, and the client tries
         * to deserialize using V4, the MapWalker constructor will throw an
         * IllegalArgumentException, because the following codes will be
         * returned from previous servers:
         *   V3: UNSUPPORTED_PROTOCOL (24)
         *   V2: BAD_PROTOCOL_MESSAGE (17)
         * Neither of these maps to any valid Nson field.
         * Convert the error to an UnsupportedProtocolException so the client's
         * serial version negotiation logic will detect it and decrement
         * the serial version accordingly.
         */
        protected static MapWalker getMapWalker(ByteInputStream in)
            throws IOException {
            int offset = in.getOffset();
            try {
                return new MapWalker(in);
            } catch (IllegalArgumentException e) {
                /* verify it was one of the two above error codes */
                in.setOffset(offset);
                int code = in.readByte();
                if (code == UNSUPPORTED_PROTOCOL ||
                    code == BAD_PROTOCOL_MESSAGE) {
                    throw new UnsupportedProtocolException(e.getMessage());
                }
                /* otherwise, throw original exception */
                throw e;
            }
        }
    }

    /**
     * Return a string from the current position of the stream, but leave
     * the offset intact. This is primarily for debugging. It is not declared
     * as private to avoid warnings when not used.
     */
    static String printNson(ByteInputStream in, boolean pretty) {
        int offset = in.getOffset();
        final String ret = JsonUtils.fromNson(in, pretty);
        in.setOffset(offset);
        return ret;
    }
}
