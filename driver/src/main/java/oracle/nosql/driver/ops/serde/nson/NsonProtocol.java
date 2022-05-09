/*-
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde.nson;

/**
 * NSON-based binary protocol
 */

public class NsonProtocol {
    /* make these easy to read - they will be shorter in practice */
    public static int V4_VERSION = 4;

    /*
     * request fields
     */
    public static String HEADER = "h";
    public static String PAYLOAD = "p";
    public static String VERSION = "v";
    public static String OP_CODE = "o";
    public static String TIMEOUT = "t";
    public static String TABLE_NAME = "n";
    public static String INDEX = "i";
    public static String INDEXES = "ix";
    public static String FIELDS = "f";
    public static String CONSISTENCY = "co";
    public static String DURABILITY = "du";
    public static String MAX_READ_KB = "mr";
    public static String MAX_WRITE_KB = "mw";
    public static String DATA = "d";
    public static String TTL = "tt";
    public static String UPDATE_TTL = "ut";
    public static String KV_VERSION = "kv";
    public static String MATCH_VERSION = "mv";
    public static String EXACT_MATCH = "ec";
    public static String IDEMPOTENT = "ip";
    public static String RETURN_ROW = "rr";
    public static String KEY = "k";
    public static String VALUE = "l";
    public static String NAME = "m";
    public static String PATH = "pt";
    public static String TYPE = "y";
    public static String IS_JSON = "j";
    public static String QUERY = "q";
    public static String PREPARE = "pp";
    public static String STATEMENT = "st";
    public static String PREPARED_STATEMENT = "ps";
    public static String WRITE_MULTIPLE = "wm";
    public static String ABORT_ON_FAIL = "a";
    public static String RANGE = "rg";
    public static String RANGE_PATH = "rp";
    public static String START = "sr";
    public static String END = "en";
    public static String INCLUSIVE = "in";
    public static String COMPARTMENT_OCID = "cc";
    public static String TABLE_OCID = "to";
    public static String TABLE_DDL = "td";
    public static String NAMESPACE = "ns";
    public static String OPERATION_ID = "od";
    public static String OPERATIONS = "os";
    public static String MAX_SHARD_USAGE_PERCENT = "ms";
    public static String NUM_OPERATIONS = "no";
    public static String IDENTITY_CACHE_SIZE = "ic";
    public static String TOPO_SEQ_NUM = "ts";
    public static String SHARD_ID = "si";
    public static String NUMBER_LIMIT = "nl";
    public static String IS_PREPARED = "is";
    public static String QUERY_VERSION = "qv";
    public static String TRACE_LEVEL = "tl";
    public static String PREPARED_QUERY = "pq";
    public static String IS_SIMPLE_QUERY = "iq";
    public static String CONTINUATION_KEY = "ck";
    public static String BIND_VARIABLES = "bv";
    public static String GET_QUERY_PLAN = "gq";
    public static String GET_QUERY_SCHEMA = "gs";
    public static String LIST_START_INDEX = "ls";
    public static String LIST_MAX_TO_READ = "lx";
    public static String LAST_INDEX = "li";
    public static String TABLES = "tb";
    public static String TABLE_USAGE = "u";
    public static String FREE_FORM_TAGS = "ff";
    public static String DEFINED_TAGS = "dt";
    public static String ETAG = "et";
    public static String TABLE_USAGE_PERIOD = "pd";
    public static String READ_THROTTLE_COUNT = "rt";
    public static String WRITE_THROTTLE_COUNT = "wt";
    public static String STORAGE_THROTTLE_COUNT = "sl";

    /*
     * response fields
     */
    public static String ERROR_CODE = "e";
    public static String EXCEPTION = "x";
    public static String RETRY_HINT = "rh";
    public static String SUCCESS = "ss";
    public static String WM_SUCCESS = "ws";
    public static String WM_FAILURE = "wf";
    public static String WM_FAIL_INDEX = "wi";
    public static String WM_FAIL_RESULT = "wr";
    public static String NUM_DELETIONS = "nd";

    // table metadata
    public static String TABLE_STATE = "as";
    public static String TABLE_SCHEMA = "ac";

    /* system request */
    public static String SYSOP_STATE = "ta";
    public static String SYSOP_RESULT = "rs";

    // throughput used and limits
    public static String LIMITS = "lm";
    public static String LIMITS_MODE = "mo";
    public static String CONSUMED = "c";
    public static String READ_UNITS = "ru";
    public static String WRITE_UNITS = "wu";
    public static String READ_KB = "rk";
    public static String WRITE_KB = "wk";
    public static String STORAGE_GB = "sg";
    // row metadata
    public static String EXPIRATION = "xp";
    public static String MODIFIED = "md";
    public static String ROW_VERSION = "rv";
    public static String ROW = "r";
    // operation metadata
    public static String GENERATED = "gn";
    public static String RETURN_INFO = "ri";
    public static String EXISTING_MOD_TIME = "em";
    public static String EXISTING_VERSION = "ev";
    public static String EXISTING_VALUE = "el";
    // RETURN_ROW defined above

    /* query response fields */
    public static String QUERY_RESULTS = "qr";
    public static String QUERY_PLAN_STRING = "qs";
    public static String QUERY_RESULT_SCHEMA = "qc";
    public static String SORT_PHASE1_RESULTS = "p1";
    public static String NUM_RESULTS = "nr";
    public static String DRIVER_QUERY_PLAN = "dq";
    public static String REACHED_LIMIT = "re";
    public static String TOPOLOGY_INFO = "tp";
    public static String TABLE_ACCESS_INFO = "ai";
    public static String NOT_TARGET_TABLES = "nt";
    public static String QUERY_OPERATION = "qo";
    public static String PROXY_TOPO_SEQNUM = "pn";
    public static String SHARD_IDS = "sa";
    public static String MATH_CONTEXT_CODE = "mc";
    public static String MATH_CONTEXT_PRECISION = "cp";
    public static String MATH_CONTEXT_ROUNDING_MODE = "rm";
}
