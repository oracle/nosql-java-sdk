/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

/**
 * Protocol constants that are part of the Proxy binary protocol
 */
public final class BinaryProtocol {

    public static final short V1 = 1;

    public static final short QUERY_V1 = 1;

    public static final short V2 = 2;

    public static final short V3 = 3;

    public static final short V4 = 4;

    /**
     * Default serial version of the protocol.
     * Note the actual version used may be less if the
     * driver is connected to an older proxy.
     */
    public static final short DEFAULT_SERIAL_VERSION = V4;

    /**
     * Serial version of the sub-protocol related to queries
     */
    public static final short QUERY_VERSION = 2;

    /**
     * Operation codes
     */
    public static enum OpCode {
        DELETE(0),
        DELETE_IF_VERSION(1),
        GET(2),
        PUT(3),
        PUT_IF_ABSENT(4),
        PUT_IF_PRESENT(5),
        PUT_IF_VERSION(6),
        QUERY(7),
        PREPARE(8),
        WRITE_MULTIPLE(9),
        MULTI_DELETE(10),
        GET_TABLE(11),
        GET_INDEXES(12),
        GET_TABLE_USAGE(13),
        LIST_TABLES(14),
        TABLE_REQUEST(15),
        SCAN(16),
        INDEX_SCAN(17),
        CREATE_TABLE(18),
        ALTER_TABLE(19),
        DROP_TABLE(20),
        CREATE_INDEX(21),
        DROP_INDEX(22),
        /* added in V2 */
        SYSTEM_REQUEST(23),
        SYSTEM_STATUS_REQUEST(24),
        /* unused ops, to be consistent with op code defined in proxy */
        UNUSED_25(25),
        UNUSED_26(26),
        UNUSED_27(27),
        UNUSED_28(28),
        UNUSED_29(29),
        UNUSED_30(30),
        UNUSED_31(31),
        UNUSED_32(32),
        ADD_REPLICA(33),
        DROP_REPLICA(34),
        GET_REPLICA_STATS(35);

        private static final OpCode[] VALUES = values();
        OpCode(int code) {
            if (code != ordinal()) {
                throw new IllegalArgumentException("Wrong op code");
            }
        }

        public static OpCode getOP(int code) {
            try {
                return VALUES[code];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("unknown opcode: " + code);
            }
        }

        public static boolean isTenantOperation(OpCode op) {
            if (op == LIST_TABLES || op == CREATE_TABLE) {
                return true;
            }
            return false;
        }
    }

    /**
     * Consistency
     */
    public static final int ABSOLUTE = 0;
    public static final int EVENTUAL = 1;

    /**
     * Durability.
     * note 1-offset is to distinguish between 0 (not set)
     * and a purposefully set value
     */
    /* sync policy */
    public static final int DURABILITY_SYNC = 1;
    public static final int DURABILITY_NO_SYNC = 2;
    public static final int DURABILITY_WRITE_NO_SYNC = 3;
    /* ack policy */
    public static final int DURABILITY_ALL = 1;
    public static final int DURABILITY_NONE = 2;
    public static final int DURABILITY_SIMPLE_MAJORITY = 3;

    /**
     * Table state
     */
    public static final int ACTIVE = 0;
    public static final int CREATING = 1;
    public static final int DROPPED = 2;
    public static final int DROPPING = 3;
    public static final int UPDATING = 4;

    /**
     * Table Limits mode
     */
    public static final int PROVISIONED = 1;
    public static final int ON_DEMAND = 2;

    /**
     * Operation state
     */
    public static final int COMPLETE = 0;
    public static final int WORKING = 1;

    /*
     * Response error codes (must be non-zero)
     */
    public static final int NO_ERROR = 0;

    /*
     * Error code range constants
     */
    public static final int USER_ERROR_BEGIN = 1;
    public static final int USER_ERROR_END = 49;
    public static final int THROTTLING_ERROR_BEGIN = 50;
    public static final int THROTTLING_ERROR_END = 99;
    public static final int SERVER_ERROR_BEGIN = 100;
    public static final int SERVER_RETRYABLE_ERROR_BEGIN = SERVER_ERROR_BEGIN;
    public static final int SERVER_RETRYABLE_ERROR_END = 124;
    public static final int SERVER_OTHER_ERROR_BEGIN = 125;

    /*
     * Error codes for user-generated errors, range from 1 to 50(exclusive).
     * These include illegal arguments, exceeding size limits for some objects,
     * resource not found, etc.
     */
    public static final int UNKNOWN_OPERATION = 1;
    public static final int TABLE_NOT_FOUND = 2;
    public static final int INDEX_NOT_FOUND = 3;
    public static final int ILLEGAL_ARGUMENT = 4;
    public static final int ROW_SIZE_LIMIT_EXCEEDED = 5;
    public static final int KEY_SIZE_LIMIT_EXCEEDED = 6;
    public static final int BATCH_OP_NUMBER_LIMIT_EXCEEDED = 7;
    public static final int REQUEST_SIZE_LIMIT_EXCEEDED = 8;
    public static final int TABLE_EXISTS = 9;
    public static final int INDEX_EXISTS = 10;
    public static final int INVALID_AUTHORIZATION = 11;
    public static final int INSUFFICIENT_PERMISSION = 12;
    public static final int RESOURCE_EXISTS = 13;
    public static final int RESOURCE_NOT_FOUND = 14;
    public static final int TABLE_LIMIT_EXCEEDED = 15;
    public static final int INDEX_LIMIT_EXCEEDED = 16;
    public static final int BAD_PROTOCOL_MESSAGE = 17;
    public static final int EVOLUTION_LIMIT_EXCEEDED = 18;
    public static final int TABLE_DEPLOYMENT_LIMIT_EXCEEDED = 19;
    public static final int TENANT_DEPLOYMENT_LIMIT_EXCEEDED = 20;
    /* added in V2 */
    public static final int OPERATION_NOT_SUPPORTED = 21;
    public static final int ETAG_MISMATCH = 22;
    public static final int CANNOT_CANCEL_WORK_REQUEST = 23;
    /* added in V3 */
    public static final int UNSUPPORTED_PROTOCOL = 24;
    /* added in V4 */
    public static final int TABLE_NOT_READY = 26;

    /*
     * Error codes for user throttling, range from 50 to 100(exclusive).
     */
    public static final int READ_LIMIT_EXCEEDED = 50;
    public static final int WRITE_LIMIT_EXCEEDED = 51;
    public static final int SIZE_LIMIT_EXCEEDED = 52;
    public static final int OPERATION_LIMIT_EXCEEDED = 53;

    /*
     * Error codes for server issues, range from 100 to 150(exclusive).
     */

    /*
     * Retry-able server issues, range from 100 to 125(exclusive).
     * These are internal problems, presumably temporary, and need to be sent
     * back to the application for retry.
     */
    public static final int REQUEST_TIMEOUT = 100;
    public static final int SERVER_ERROR = 101;
    public static final int SERVICE_UNAVAILABLE = 102;
    public static final int SECURITY_INFO_UNAVAILABLE = 104;
    /* added in V2 */
    public static final int RETRY_AUTHENTICATION = 105;

    /*
     * Other server issues, begin from 125.
     * These include server illegal state, unknown server error, etc.
     * They might be retry-able, or not.
     */
    public static final int UNKNOWN_ERROR = 125;
    public static final int ILLEGAL_STATE = 126;

    /*
     * Return true if the errorCode means a user-generated error.
     */
    public static boolean isUserFailure(int errorCode) {
        return errorCode >= USER_ERROR_BEGIN &&
               errorCode <= USER_ERROR_END;
    }

    /*
     * Return true if the errorCode means a failure caused by user limits.
     */
    public static boolean isUserThrottling(int errorCode) {
        return errorCode >= THROTTLING_ERROR_BEGIN &&
               errorCode <= THROTTLING_ERROR_END;
    }

    /*
     * Return true if the errorCode means a server failure.
     */
    public static boolean isServerFailure(int errorCode) {
        return errorCode >= SERVER_ERROR_BEGIN;
    }

    /*
     * Return true if the errorCode means a retry-able server failure.
     */
    public static boolean isServerRetryableFailure(int errorCode) {
        return errorCode >= SERVER_RETRYABLE_ERROR_BEGIN &&
               errorCode <= SERVER_RETRYABLE_ERROR_END;
    }

    /*
     * TTL units
     */
    public static final int TTL_HOURS = 1;
    public static final int TTL_DAYS = 2;

    /*
     * Limits
     */

    /* The row size limit */
    public static final int ROW_SIZE_LIMIT = 512 * 1024;

    /* The key size limit */
    public static final int KEY_SIZE_LIMIT = 64;

    /* The index key size limit, independent of primary key */
    public static final int INDEX_KEY_SIZE_LIMIT = 64;

    /* The max number of operations in a WriteMultiple request */
    public static final int BATCH_OP_NUMBER_LIMIT = 50;

    /* The max size of WriteMultiple request. */
    public static final int BATCH_REQUEST_SIZE_LIMIT = 25 * 1024 * 1024;

    /* The max size of request */
    public static final int REQUEST_SIZE_LIMIT = 2 * 1024 * 1024;

    /* The limit on the max read KB during a operation */
    public static final int READ_KB_LIMIT = 2 * 1024;

    /* The limit on the max write KB during a operation */
    public static final int WRITE_KB_LIMIT = 2 * 1024;

    /* The limit on a query string */
    public static final int QUERY_SIZE_LIMIT = 10 * 1024;
}
