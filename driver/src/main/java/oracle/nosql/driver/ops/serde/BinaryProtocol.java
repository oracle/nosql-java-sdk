/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import static oracle.nosql.driver.http.Client.trace;
import static oracle.nosql.driver.util.BinaryProtocol.ABSOLUTE;
import static oracle.nosql.driver.util.BinaryProtocol.ACTIVE;
import static oracle.nosql.driver.util.BinaryProtocol.ON_DEMAND;
import static oracle.nosql.driver.util.BinaryProtocol.BAD_PROTOCOL_MESSAGE;
import static oracle.nosql.driver.util.BinaryProtocol.BATCH_OP_NUMBER_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.BATCH_REQUEST_SIZE_LIMIT;
import static oracle.nosql.driver.util.BinaryProtocol.COMPLETE;
import static oracle.nosql.driver.util.BinaryProtocol.CREATING;
import static oracle.nosql.driver.util.BinaryProtocol.DROPPED;
import static oracle.nosql.driver.util.BinaryProtocol.DROPPING;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_WRITE_NO_SYNC;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_ALL;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_NONE;
import static oracle.nosql.driver.util.BinaryProtocol.DURABILITY_SIMPLE_MAJORITY;
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
import static oracle.nosql.driver.util.BinaryProtocol.PROVISIONED;
import static oracle.nosql.driver.util.BinaryProtocol.READ_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_SIZE_LIMIT;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.REQUEST_TIMEOUT;
import static oracle.nosql.driver.util.BinaryProtocol.RESOURCE_EXISTS;
import static oracle.nosql.driver.util.BinaryProtocol.RESOURCE_NOT_FOUND;
import static oracle.nosql.driver.util.BinaryProtocol.RETRY_AUTHENTICATION;
import static oracle.nosql.driver.util.BinaryProtocol.ROW_SIZE_LIMIT_EXCEEDED;
import static oracle.nosql.driver.util.BinaryProtocol.SECURITY_INFO_UNAVAILABLE;
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
import static oracle.nosql.driver.util.BinaryProtocol.UNKNOWN_ERROR;
import static oracle.nosql.driver.util.BinaryProtocol.UNKNOWN_OPERATION;
import static oracle.nosql.driver.util.BinaryProtocol.UNSUPPORTED_PROTOCOL;
import static oracle.nosql.driver.util.BinaryProtocol.UPDATING;
import static oracle.nosql.driver.util.BinaryProtocol.V2;
import static oracle.nosql.driver.util.BinaryProtocol.V3;
import static oracle.nosql.driver.util.BinaryProtocol.WORKING;
import static oracle.nosql.driver.util.BinaryProtocol.WRITE_LIMIT_EXCEEDED;

import java.io.IOException;

import oracle.nosql.driver.BatchOperationNumberLimitException;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.DeploymentException;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.EvolutionLimitException;
import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.IndexLimitException;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.KeySizeLimitException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.Nson;
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
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.ReadRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableLimits.CapacityMode;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.query.TopologyInfo;
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
public class BinaryProtocol extends Nson {

    /*
     * Serialization
     *
     * Methods related to data come from Nson. Methods here refer
     * to objects in the prototol other than data
     */

    static void writeTimeout(ByteOutputStream out, int timeout)
        throws IOException {

        SerializationUtil.writePackedInt(out, timeout);
    }

    static void writeConsistency(ByteOutputStream out,
                                 Consistency consistency)
        throws IOException {

        out.writeByte(getConsistency(consistency));
    }

    static void writeDurability(ByteOutputStream out,
                                Durability durability,
                                short serialVersion)
        throws IOException {

        if (serialVersion > V2) {
            out.writeByte(getDurability(durability));
        }
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

    /**
     * Writes the mode for TableLimits.
     * @param out output
     * @param mode table limits mode
     * @throws IOException if exception
     */
    static void writeCapacityMode(ByteOutputStream out,
                                  CapacityMode mode,
                                  short serialVersion)
        throws IOException {

        if (serialVersion < V3) {
            return;
        }

        switch (mode) {
        case PROVISIONED:
            out.writeByte(PROVISIONED);
            break;
        case ON_DEMAND:
            out.writeByte(ON_DEMAND);
            break;
        }
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

    public static void writeVersion(ByteOutputStream out,
                                    Version version) throws IOException {
        SerializationUtil.writeNonNullByteArray(out, version.getBytes());
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
                                      ByteOutputStream out,
                                      short serialVersion)
        throws IOException {

        serializeRequest(writeRq, out);
        writeString(out, writeRq.getTableName());
        out.writeBoolean(writeRq.getReturnRowInternal());
        writeDurability(out, writeRq.getDurability(),
                        serialVersion);
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
                                         WriteResult result,
                                         short serialVersion)
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
        if (serialVersion > V2) {
            result.setExistingModificationTime(readLong(in));
        } else {
            result.setExistingModificationTime(-1);
        }
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

    static TableResult deserializeTableResult(ByteInputStream in,
                                              short serialVersion)
        throws IOException {

        TableResult result = new TableResult();
        boolean hasInfo = in.readBoolean();
        if (hasInfo) {
            result.setCompartmentId(readString(in));
            result.setTableName(readString(in));
            result.setState(getTableState(in.readByte()));
            boolean hasStaticState = in.readBoolean();
            if (hasStaticState) {
                int readKb = readInt(in);
                int writeKb = readInt(in);
                int storageGB = readInt(in);
                CapacityMode mode;
                if (serialVersion > V2) {
                    mode = getCapacityMode(in.readByte());
                } else {
                    mode = CapacityMode.PROVISIONED;
                }
                /*
                 * on-prem tables may return all 0 because of protocol
                 * limitations that lump the schema with limits. Return
                 * null to user for those cases.
                 */
                if (!(readKb == 0 && writeKb == 0 && storageGB == 0)) {
                    result.setTableLimits(
                        new TableLimits(readKb, writeKb, storageGB, mode));
                }
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
            /* No topology info sent by proxy */
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
            if (msg.contains("Invalid driver serial version")) {
                return new UnsupportedProtocolException(msg);
            }
            return new IllegalArgumentException("Bad protocol message: " + msg);
        case UNSUPPORTED_PROTOCOL:
            /* note this is specifically for protocol version mismatches */
            return new UnsupportedProtocolException(msg);
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

    private static int getDurability(Durability durability) {
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

    public static TableResult.State getTableState(int state) {
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

    static TableLimits.CapacityMode getCapacityMode(int mode) {
        switch (mode) {
        case PROVISIONED:
            return TableLimits.CapacityMode.PROVISIONED;
        case ON_DEMAND:
            return TableLimits.CapacityMode.ON_DEMAND;
        default:
            throw new IllegalStateException("Unknown capacity mode " + mode);
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
}
