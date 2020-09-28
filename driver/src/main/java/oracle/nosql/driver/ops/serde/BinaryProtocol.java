/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import static oracle.nosql.driver.http.Client.trace;
import static oracle.nosql.driver.util.BinaryProtocol.ABSOLUTE;
import static oracle.nosql.driver.util.BinaryProtocol.ACTIVE;
import static oracle.nosql.driver.util.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.driver.util.BinaryProtocol.BATCH_OP_NUMBER_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.BATCH_REQUEST_SIZE_LIMIT;
import static oracle.nosql.driver.util.BinaryProtocol.CREATING;
import static oracle.nosql.driver.util.BinaryProtocol.COMPLETE;
import static oracle.nosql.driver.util.BinaryProtocol.DROPPED;
import static oracle.nosql.driver.util.BinaryProtocol.DROPPING;
import static oracle.nosql.driver.util.BinaryProtocol.EVENTUAL;
import static oracle.nosql.driver.util.BinaryProtocol.EVOLUTION_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.driver.util.BinaryProtocol.ILLEGAL_STATE;
import static oracle.nosql.driver.util.BinaryProtocol.INDEX_EXISTS;
import static oracle.nosql.driver.util.BinaryProtocol.INDEX_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.INDEX_NOT_FOUND;
import static oracle.nosql.driver.util.BinaryProtocol.INSUFFICIENT_PERMISSION;
import static oracle.nosql.driver.util.BinaryProtocol.INVALID_AUTHORIZATION;
import static oracle.nosql.driver.util.BinaryProtocol.KEY_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.OPERATION_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.OPERATION_NOT_SUPPORTED;
import static oracle.nosql.driver.util.BinaryProtocol.READ_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_SIZE_LIMIT;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_TIMEOUT;
import static oracle.nosql.driver.util.BinaryProtocol.RESOURCE_EXISTS;
import static oracle.nosql.driver.util.BinaryProtocol.RESOURCE_NOT_FOUND;
import static oracle.nosql.driver.util.BinaryProtocol.RETRY_AUTHENTICATION;
import static oracle.nosql.driver.util.BinaryProtocol.ROW_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.SECURITY_INFO_UNAVAILABLE;
import static oracle.nosql.driver.util.BinaryProtocol.SERIAL_VERSION;
import static oracle.nosql.driver.util.BinaryProtocol.SERVER_ERROR;
import static oracle.nosql.driver.util.BinaryProtocol.SERVICE_UNAVAILABLE;
import static oracle.nosql.driver.util.BinaryProtocol.SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.TABLE_DEPLOYMENT_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.TABLE_EXISTS;
import static oracle.nosql.driver.util.BinaryProtocol.TABLE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.TABLE_NOT_FOUND;
import static oracle.nosql.driver.util.BinaryProtocol.TENANT_DEPLOYMENT_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.TTL_DAYS;
import static oracle.nosql.driver.util.BinaryProtocol.TTL_HOURS;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_ARRAY;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_BINARY;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_DOUBLE;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_EMPTY;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_JSON_NULL;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_LONG;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_NULL;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_NUMBER;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.driver.util.BinaryProtocol.TYPE_TIMESTAMP;
import static oracle.nosql.driver.util.BinaryProtocol.UNKNOWN_ERROR;
import static oracle.nosql.driver.util.BinaryProtocol.UNKNOWN_OPERATION;
import static oracle.nosql.driver.util.BinaryProtocol.UPDATING;
import static oracle.nosql.driver.util.BinaryProtocol.WORKING;
import static oracle.nosql.driver.util.BinaryProtocol.WRITE_LIMIT_EXCEEDED;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

import oracle.nosql.driver.BatchOperationNumberLimitException;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.DeploymentException;
import oracle.nosql.driver.EvolutionLimitException;
import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.IndexLimitException;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.KeySizeLimitException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.OperationNotSupportedException;
import oracle.nosql.driver.OperationThrottlingException;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.RequestSizeLimitException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.ResourceExistsException;
import oracle.nosql.driver.ResourceNotFoundException;
import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.SystemException;
import oracle.nosql.driver.TableExistsException;
import oracle.nosql.driver.TableLimitException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TableSizeException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.UnauthorizedException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.ReadRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.EmptyValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValueEventHandler;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * @hidden
 * A base class for binary protocol serialization and constant protocol values.
 * Constants are used instead of relying on ordering of values in enumerations
 * or other derived protocol state.
 */
public class BinaryProtocol {

    /**
     * Serial version of the protocol
     * @return the version
     */
    public static short getSerialVersion() {
        return SERIAL_VERSION;
    }

    /*
     * Serialization
     */

    /*
     * Primitives
     */
    static void writeInt(ByteOutputStream out,
                         int value) throws IOException {
        SerializationUtil.writePackedInt(out, value);
    }

    static void writeLong(ByteOutputStream out,
                          long value) throws IOException {
        SerializationUtil.writePackedLong(out, value);
    }

    static void writeDouble(ByteOutputStream out,
                            double value) throws IOException {
        out.writeDouble(value);
    }

    static void writeTimestamp(ByteOutputStream out,
                               TimestampValue value) throws IOException {
        /* TODO, temporary */
        writeString(out, value.getString());
    }

    static void writeString(ByteOutputStream out,
                            String s) throws IOException {

        SerializationUtil.writeString(out, s);
    }

    /**
     * Writes a char array as a UTF8 byte array. This is used for
     * system queries that may contain a password.
     */
    static void writeCharArrayAsUTF8(ByteOutputStream out,
                                     char [] chars) throws IOException {
        ByteBuffer buf = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars));
        byte[] array = new byte[buf.limit()];
        buf.get(array);
        writeByteArray(out, array);
    }

    static void writeTimeout(ByteOutputStream out, int timeout)
        throws IOException {

        SerializationUtil.writePackedInt(out, timeout);
    }

    static void writeConsistency(ByteOutputStream out,
                                 Consistency consistency)
        throws IOException {

        out.writeByte(getConsistency(consistency));
    }

    static void writeTTL(ByteOutputStream out, TimeToLive ttl)
        throws IOException {

        if (ttl == null) {
            writeLong(out, -1);
            return;
        }
        writeLong(out, ttl.getValue());
        if (ttl.unitIsDays()) {
            out.writeByte(TTL_DAYS);
        } else if (ttl.unitIsHours()) {
            out.writeByte(TTL_HOURS);
        } else {
            throw new IllegalStateException("Invalid TTL unit in ttl " + ttl);
        }
    }

    public static int getSerializedSize(FieldValue value) {
        ByteOutputStream out = null;
        try {
            out = ByteOutputStream.createByteOutputStream();
            writeFieldValue(out, value);
            int ret = out.getOffset();
            return ret;
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Can't serialize field value: " + ioe.getMessage());
        } finally {
            if (out != null) {
                out.releaseByteBuf();
            }
        }
    }

    /**
     * Writes the (short) serial version
     * @param out output
     * @throws IOException if exception
     */
    public static void writeSerialVersion(ByteOutputStream out)
        throws IOException {
        out.writeShort(getSerialVersion());
    }

    /**
     * Writes the opcode for the operation.
     * @param out output
     * @param op opCode
     * @throws IOException if exception
     */
    static void writeOpCode(ByteOutputStream out, OpCode op)
        throws IOException {

        out.writeByte(op.ordinal());
    }

    static void writeFieldRange(ByteOutputStream out,
                                FieldRange range)
        throws IOException {

        if (range == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);

        writeString(out, range.getFieldPath());

        if (range.getStart() != null) {
            out.writeBoolean(true);
            writeFieldValue(out, range.getStart());
            out.writeBoolean(range.getStartInclusive());
        } else {
            out.writeBoolean(false);
        }

        if (range.getEnd() != null) {
            out.writeBoolean(true);
            writeFieldValue(out, range.getEnd());
            out.writeBoolean(range.getEndInclusive());
        } else {
            out.writeBoolean(false);
        }
    }

    /*
     * Serialize a generic FieldValue into the output stream
     */
    public static void writeFieldValue(ByteOutputStream out,
                                       FieldValue value)
        throws IOException {

        BinarySerializer bs = new BinarySerializer(out);
        FieldValueEventHandler.generate(value, bs);
    }

    public static void writeVersion(ByteOutputStream out,
                                    Version version) throws IOException {
        SerializationUtil.writeNonNullByteArray(out, version.getBytes());
    }

    public static void writeByteArray(ByteOutputStream out,
                                      byte[] array) throws IOException {
        SerializationUtil.writeByteArray(out, array);
    }

    /*
     * Writes a byte array with a full 4-byte int length
     */
    public static void writeByteArrayWithInt(ByteOutputStream out,
                                             byte[] array) throws IOException {
        out.writeInt(array.length);
        out.write(array);
    }

    /*
     * Writes fields from ReadRequest
     */
    static void serializeReadRequest(ReadRequest readRq,
                                     ByteOutputStream out)
        throws IOException {

        serializeRequest(readRq, out);
        writeString(out, readRq.getTableName());
        writeConsistency(out, readRq.getConsistencyInternal());
    }

    /*
     * Writes fields from WriteRequest
     */
    static void serializeWriteRequest(WriteRequest writeRq,
                                      ByteOutputStream out)
        throws IOException {

        serializeRequest(writeRq, out);
        writeString(out, writeRq.getTableName());
        out.writeBoolean(writeRq.getReturnRowInternal());
    }

    /*
     * Writes fields from Request
     */
    static void serializeRequest(Request rq,
                                 ByteOutputStream out)
        throws IOException {

        writeTimeout(out, rq.getTimeoutInternal());
    }

    /*
     * Deserialization
     */

    /*
     * Primitives
     */
    static int readInt(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedInt(in);
    }

    static long readLong(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedLong(in);
    }

    static double readDouble(ByteInputStream in) throws IOException {
        return in.readDouble();
    }

    static String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    static byte[] readByteArray(ByteInputStream in) throws IOException {
        return SerializationUtil.readByteArray(in);
    }

    static int[] readIntArray(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedIntArray(in);
    }

    /**
     * Reads a byte array that has a not-packed integer size
     */
    static byte[] readByteArrayWithInt(ByteInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0) {
            throw new IOException(
                "Invalid length for prepared query: " + length);
        }
        byte[] query = new byte[length];
        in.readFully(query);
        return query;
    }

    static Version readVersion(ByteInputStream in) throws IOException {
        return Version.createVersion(readByteArray(in));
    }

    static void deserializeConsumedCapacity(ByteInputStream in,
                                            Result result)
        throws IOException {

        result.setReadUnits(SerializationUtil.readPackedInt(in));
        result.setReadKB(SerializationUtil.readPackedInt(in));
        result.setWriteKB(SerializationUtil.readPackedInt(in));
    }

    static void deserializeWriteResponse(ByteInputStream in,
                                         WriteResult result)
        throws IOException {

        boolean returnInfo = in.readBoolean();
        if (!returnInfo) {
            return;
        }

        /*
         * Existing info always includes both value and version
         */
        result.setExistingValue(readFieldValue(in).asMap());
        result.setExistingVersion(readVersion(in));
    }

    static void deserializeGeneratedValue(ByteInputStream in,
                                         PutResult result)
        throws IOException {

        boolean hasGeneratedValue = in.readBoolean();
        if (!hasGeneratedValue) {
            return;
        }
        result.setGeneratedValue(readFieldValue(in));
    }

    static TableResult deserializeTableResult(ByteInputStream in)
        throws IOException {

        TableResult result = new TableResult();
        boolean hasInfo = in.readBoolean();
        if (hasInfo) {
            result.setDomainId(readString(in));
            result.setTableName(readString(in));
            result.setState(getTableState(in.readByte()));
            boolean hasStaticState = in.readBoolean();
            if (hasStaticState) {
                int readKb = readInt(in);
                int writeKb = readInt(in);
                int storageGB = readInt(in);
                TableLimits limits = new TableLimits(readKb, writeKb, storageGB);
                result.setTableLimits(limits);
                result.setSchema(readString(in));
            }
            result.setOperationId(readString(in));
        }

        return result;
    }

    /**
     *  state, operation, statement
     */
    static SystemResult deserializeSystemResult(ByteInputStream in)
        throws IOException {

        SystemResult result = new SystemResult();
        result.setState(getOperationState(in.readByte()));
        result.setOperationId(readString(in));
        result.setStatement(readString(in));
        result.setResultString(readString(in));
        return result;
    }

    public static TopologyInfo readTopologyInfo(ByteInputStream in)
        throws IOException {

        int seqNum = SerializationUtil.readPackedInt(in);

        trace("readTopologyInfo: seqNum =  " + seqNum, 4);

        if (seqNum < -1) {
            throw new IOException(
                "Invalid topology sequence number: " + seqNum);
        }

        if (seqNum == -1) {
            // No topology info sent by proxy
            return null;
        }

        int[] shardIds = SerializationUtil.readPackedIntArray(in);

        return new TopologyInfo(seqNum, shardIds);
    }

    /*
     * Maps the error code returned from the server into a local string.
     */
    public static RuntimeException mapException(int code, String msg) {
        switch (code) {
        case UNKNOWN_ERROR:
        case UNKNOWN_OPERATION:
            return new NoSQLException("Unknown error: " + msg);
        case TABLE_NOT_FOUND:
            return new TableNotFoundException(msg);
        case INDEX_LIMIT_EXCEEDED:
            return new IndexLimitException(msg);
        case TABLE_LIMIT_EXCEEDED:
            return new TableLimitException(msg);
        case EVOLUTION_LIMIT_EXCEEDED:
            return new EvolutionLimitException(msg);
        case INDEX_NOT_FOUND:
            return new IndexNotFoundException(msg);
        case ILLEGAL_ARGUMENT:
            return new IllegalArgumentException(msg);
        case READ_LIMIT_EXCEEDED:
            return new ReadThrottlingException(msg);
        case WRITE_LIMIT_EXCEEDED:
            return new WriteThrottlingException(msg);
        case SIZE_LIMIT_EXCEEDED:
            return new TableSizeException(msg);
        case ROW_SIZE_LIMIT_EXCEEDED:
            return new RowSizeLimitException(msg);
        case KEY_SIZE_LIMIT_EXCEEDED:
            return new KeySizeLimitException(msg);
        case BATCH_OP_NUMBER_LIMIT_EXCEEDED:
            return new BatchOperationNumberLimitException(msg);
        case REQUEST_SIZE_LIMIT_EXCEEDED:
            return new RequestSizeLimitException(msg);
        case TABLE_EXISTS:
            return new TableExistsException(msg);
        case INDEX_EXISTS:
            return new IndexExistsException(msg);
        case TABLE_DEPLOYMENT_LIMIT_EXCEEDED:
        case TENANT_DEPLOYMENT_LIMIT_EXCEEDED:
            return new DeploymentException(msg);
        case ILLEGAL_STATE:
            return new IllegalStateException(msg);
        case SERVICE_UNAVAILABLE:
        case SERVER_ERROR:
            /* treat above as retryable system errors */
            return new SystemException(msg);
        case BAD_PROTOCOL_MESSAGE:
            return new IllegalArgumentException("Bad protocol message: " + msg);
        case REQUEST_TIMEOUT:
            return new RequestTimeoutException(msg);
        case INVALID_AUTHORIZATION:
            return new InvalidAuthorizationException(msg);
        case INSUFFICIENT_PERMISSION:
            return new UnauthorizedException(msg);
        case SECURITY_INFO_UNAVAILABLE:
            return new SecurityInfoNotReadyException(msg);
        case RETRY_AUTHENTICATION:
            return new AuthenticationException(msg);
        case OPERATION_LIMIT_EXCEEDED:
            return new OperationThrottlingException(msg);
        case RESOURCE_EXISTS:
            return new ResourceExistsException(msg);
        case RESOURCE_NOT_FOUND:
            return new ResourceNotFoundException(msg);
        case OPERATION_NOT_SUPPORTED:
            return new OperationNotSupportedException(msg);
        default:
            return new NoSQLException("Unknown error code " + code + ": " +
                                      msg);
        }
    }

    private static int getConsistency(Consistency consistency) {
        if (consistency == Consistency.ABSOLUTE) {
            return ABSOLUTE;
        }
        return EVENTUAL;
    }

    static TableResult.State getTableState(int state) {
        switch (state) {
        case ACTIVE:
            return TableResult.State.ACTIVE;
        case CREATING:
            return TableResult.State.CREATING;
        case DROPPED:
            return TableResult.State.DROPPED;
        case DROPPING:
            return TableResult.State.DROPPING;
        case UPDATING:
            return TableResult.State.UPDATING;
        default:
            throw new IllegalStateException("Unknown table state " + state);
        }
    }

    static SystemResult.State getOperationState(int state) {
        switch (state) {
        case COMPLETE:
            return SystemResult.State.COMPLETE;
        case WORKING:
            return SystemResult.State.WORKING;
        default:
            throw new IllegalStateException("Unknown operation state " + state);
        }
    }

    /*
     * Checks if the request size exceeds the limit.
     */
    public static void checkRequestSizeLimit(Request request, int requestSize) {
        if (!request.getCheckRequestSize()) {
            return;
        }
        final int requestSizeLimit =
            (request instanceof WriteMultipleRequest) ?
            BATCH_REQUEST_SIZE_LIMIT : REQUEST_SIZE_LIMIT;

        if (requestSize > requestSizeLimit) {
            throw new RequestSizeLimitException("The request size of " +
                requestSize + " exceeded the limit of " + requestSizeLimit);
        }
    }

    /**
     * An instance of FieldValueEventHandler that accepts events and adds them
     * to the protocol output stream.
     */
    private static class BinarySerializer implements FieldValueEventHandler {

        private final ByteOutputStream out;

        /*
         * Stack used to store offsets for maps and arrays for tracking
         * number of bytes used for a map or array.
         */
        private final Stack<Integer> offsetStack;
        /*
         * Stack used to store sizes for maps and arrays for tracking number of
         * elements in a map or array.
         */
        private final Stack<Integer> sizeStack;

        public BinarySerializer(ByteOutputStream out) {
            this.out = out;
            offsetStack = new Stack<Integer>();
            sizeStack = new Stack<Integer>();
        }

        /*
         * Maps and Arrays. These objects start with their total length,
         * allowing them to be optionally skipped on deserialization.
         *  1. start:
         *    make a 4-byte space for the ultimate length of the serialized
         *    object.
         *  2. save the offset on a stack
         *  3. start counting elements on a stack
         *  4. ... entries are written
         *  5. end:
         *    a. pop the offset stack to get the original length offset
         *    write the real length into the spot that was held
         *    b. pop the size stack to get the number of elements
         *    write the real number of elements the spot that was held
         * NOTE: a full 4-byte integer is used to avoid the variable-length
         * encoding used by compressed integers.
         *
         * It would be more efficient and avoid an extra stack with pop/push
         * for each map/array element to rely on the size from the caller
         * but counting elements here is safer and doesn't rely on the caller
         * having access to the size information. For example, a caller may be
         * turning a List (via iterator) into an array. That is less likely
         * for a Map but it's simplest to keep them the same. Alternatively
         * the caller could track number of elements and send it correctly in
         * the end* calls but again, that relies on the caller.
         */
        @Override
        public void startMap(int size) throws IOException {
            out.writeByte(TYPE_MAP);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void startArray(int size) throws IOException {
            out.writeByte(TYPE_ARRAY);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void endMap(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public void endArray(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public void startMapField(String key) throws IOException {
            writeString(out, key);
        }

        @Override
        public void endMapField() throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void endArrayField() throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void booleanValue(boolean value) throws IOException {
            out.writeByte(TYPE_BOOLEAN);
            out.writeBoolean(value);
        }

        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            out.writeByte(TYPE_BINARY);
            writeByteArray(out, byteArray);
        }

        @Override
        public void stringValue(String value) throws IOException {
            out.writeByte(TYPE_STRING);
            writeString(out, value);
        }

        @Override
        public void integerValue(int value) throws IOException {
            out.writeByte(TYPE_INTEGER);
            writeInt(out, value);
        }

        @Override
        public void longValue(long value) throws IOException {
            out.writeByte(TYPE_LONG);
            writeLong(out, value);
        }

        @Override
        public void doubleValue(double value) throws IOException {
            out.writeByte(TYPE_DOUBLE);
            writeDouble(out, value);
        }

        @Override
        public void numberValue(BigDecimal value) throws IOException {
            out.writeByte(TYPE_NUMBER);
            writeString(out, value.toString());
        }

        @Override
        public void timestampValue(TimestampValue timestamp)
            throws IOException {

            out.writeByte(TYPE_TIMESTAMP);
            writeTimestamp(out, timestamp);
        }

        @Override
        public void jsonNullValue() throws IOException {
            out.writeByte(TYPE_JSON_NULL);
        }

        @Override
        public void nullValue() throws IOException {
            out.writeByte(TYPE_NULL);
        }

        @Override
        public void emptyValue() throws IOException {
            out.writeByte(TYPE_EMPTY);
        }

        private void incrSize() {
            int value = sizeStack.pop();
            sizeStack.push(value + 1);
        }
    }

    /**
     * @hidden
     *
     * An instance of FieldValueEventHandler that accepts events and constructs
     * a {@link FieldValue} instance. This is used for creating instances
     * from the wire protocol.
     *
     * In order to handle creation of nested complex types such as maps and
     * arrays stacks are maintained.

     * The current FieldValue instance is available using the
     * getCurrentValue() method.
     *
     * This class is public only so it can be tested.
     */
    public static class FieldValueCreator implements FieldValueEventHandler {

        private Stack<MapValue> mapStack;
        private Stack<ArrayValue> arrayStack;

        /*
         * A stack of map keys is needed to handle the situation where maps
         * are nested.
         */
        private Stack<String> keyStack;
        MapValue currentMap;
        ArrayValue currentArray;
        String currentKey;
        FieldValue currentValue;

        private void pushMap(MapValue map) {
            if (currentMap != null) {
                if (mapStack == null) {
                    mapStack = new Stack<MapValue>();
                }
                mapStack.push(currentMap);
            }
            currentMap = map;
            currentValue = map;
        }

        private void pushArray(ArrayValue array) {
            if (currentArray != null) {
                if (arrayStack == null) {
                    arrayStack = new Stack<ArrayValue>();
                }
                arrayStack.push(currentArray);
            }
            currentArray = array;
            currentValue = array;
        }

        private void pushKey(String key) {
            if (currentKey != null) {
                if (keyStack == null) {
                    keyStack = new Stack<String>();
                }
                keyStack.push(currentKey);
            }
            currentKey = key;
        }

        /**
         * Returns the current FieldValue if available
         *
         * @return the current value
         */
        public FieldValue getCurrentValue() {
            return currentValue;
        }

        @Override
        public void startMap(int size) throws IOException {
            /* maintain insertion order */
            pushMap(new MapValue(true, size));
        }

        @Override
        public void startArray(int size) throws IOException {
            pushArray(new ArrayValue(size));
        }

        @Override
        public void endMap(int size) throws IOException {
            /*
             * The in-process map becomes the currentValue
             */
            currentValue = currentMap;
            if (mapStack != null && !mapStack.empty()) {
                currentMap = mapStack.pop();
            } else {
                currentMap = null;
            }
        }

        @Override
        public void endArray(int size) throws IOException {
            /*
             * The in-process array becomes the currentValue
             */
            currentValue = currentArray;
            if (arrayStack != null && !arrayStack.empty()) {
                currentArray = arrayStack.pop();
            } else {
                currentArray = null;
            }
        }

        @Override
        public void startMapField(String key) throws IOException {
            pushKey(key);
        }

        @Override
        public void endMapField() throws IOException {
            currentMap.put(currentKey, currentValue);
            if (keyStack != null && !keyStack.empty()) {
                currentKey = keyStack.pop();
            } else {
                currentKey = null;
            }
            // currentValue undefined right now...
        }

        @Override
        public void endArrayField() throws IOException {
            currentArray.add(currentValue);
        }

        @Override
        public void booleanValue(boolean value) throws IOException {
            currentValue = BooleanValue.getInstance(value);
        }

        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            currentValue = new BinaryValue(byteArray);
        }

        @Override
        public void stringValue(String value) throws IOException {
            currentValue = new StringValue(value);
        }

        @Override
        public void integerValue(int value) throws IOException {
            currentValue = new IntegerValue(value);
        }

        @Override
        public void longValue(long value) throws IOException {
            currentValue = new LongValue(value);
        }

        @Override
        public void doubleValue(double value) throws IOException {
            currentValue = new DoubleValue(value);
        }

        @Override
        public void numberValue(BigDecimal value) throws IOException {
            currentValue = new NumberValue(value);
        }

        @Override
        public void timestampValue(TimestampValue timestamp) {
            currentValue = timestamp;
        }

        @Override
        public void jsonNullValue() throws IOException {
            currentValue = JsonNullValue.getInstance();
        }

        @Override
        public void nullValue() throws IOException {
            currentValue = NullValue.getInstance();
        }

        @Override
        public void emptyValue() throws IOException {
            currentValue = EmptyValue.getInstance();
        }
    }

    /*
     * Read the protocol input stream and send events to a handler that
     * creates a FieldValue.
     */
    public static FieldValue readFieldValue(ByteInputStream in)
        throws IOException {
        FieldValueCreator handler = new FieldValueCreator();
        readFieldValueInternal(handler, in);
        /*
         * Results accumulated in the handler
         */
        return handler.getCurrentValue();
    }

    /*
     * Internal implementation that uses the same handler to maintain state
     * while creating a FieldValue.
     */
    private static void readFieldValueInternal(
        FieldValueEventHandler handler, ByteInputStream in)
        throws IOException {


        int t = in.readByte();
        switch (t) {
        case TYPE_ARRAY:
            in.readInt(); // length of serialized bytes
            int length = in.readInt();
            handler.startArray(length);
            for (int i = 0; i < length; i++) {
                readFieldValueInternal(handler, in);
                handler.endArrayField();
            }
            handler.endArray(length);
            break;
        case TYPE_BINARY:
            handler.binaryValue(readByteArray(in));
            break;
        case TYPE_BOOLEAN:
            handler.booleanValue(in.readBoolean());
            break;
        case TYPE_DOUBLE:
            handler.doubleValue(readDouble(in));
            break;
        case TYPE_INTEGER:
            handler.integerValue(readInt(in));
            break;
        case TYPE_LONG:
            handler.longValue(readLong(in));
            break;
        case TYPE_MAP:
            in.readInt(); // length of serialized bytes
            length = in.readInt(); // size of map
            handler.startMap(length);
            for (int i = 0; i < length; i++) {
                String key = readString(in);
                handler.startMapField(key);
                readFieldValueInternal(handler, in); // read value
                handler.endMapField();
            }
            handler.endMap(length);
            break;
        case TYPE_STRING:
            handler.stringValue(readString(in));
            break;
        case TYPE_TIMESTAMP:
            handler.timestampValue(new TimestampValue(readString(in), 1));
            break;
        case TYPE_NUMBER:
            handler.numberValue(new BigDecimal(readString(in)));
            break;
        case TYPE_JSON_NULL:
            handler.jsonNullValue();
            break;
        case TYPE_NULL:
            handler.nullValue();
            break;
        case TYPE_EMPTY :
            handler.emptyValue();
            break;
        default:
            throw new IllegalStateException("Unknown value type code: " + t);
        }
    }
}
