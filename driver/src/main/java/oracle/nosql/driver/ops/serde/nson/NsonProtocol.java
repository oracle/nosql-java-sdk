/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde.nson;

import java.util.HashMap;

/**
 * NSON-based binary protocol
 */

public class NsonProtocol {
    public static int V4_VERSION = 4;

    /*
     * request fields
     *
     * These are purposely terse to keep message size down. There is a debug
     * mechanism for printing messages in a more verbose, human-readable
     * format
     */
    public static String ABORT_ON_FAIL = "a";
    public static String BATCH_COUNTER = "bc";
    public static String BIND_VARIABLES = "bv";
    public static String CDC_ENABLED = "ce";
    public static String COMPARTMENT_OCID = "cc";
    public static String CONSISTENCY = "co";
    public static String CONSUMER_METADATA = "cm";
    public static String CONSUMER_TABLES = "ct";
    public static String CONTINUATION_KEY = "ck";
    public static String CURSOR = "cu";
    public static String DATA = "d";
    public static String DEFINED_TAGS = "dt";
    public static String DRL_OPTIN = "dro";
    public static String DURABILITY = "du";
    public static String END = "en";
    public static String ETAG = "et";
    public static String EXACT_MATCH = "ec";
    public static String FIELDS = "f";
    public static String FREE_FORM_TAGS = "ff";
    public static String FORCE_RESET = "fr";
    public static String GET_QUERY_PLAN = "gq";
    public static String GET_QUERY_SCHEMA = "gs";
    public static String GROUP_ID = "gr";
    public static String HEADER = "h";
    public static String IDEMPOTENT = "ip";
    public static String IDENTITY_CACHE_SIZE = "ic";
    public static String INCLUSIVE = "in";
    public static String INDEX = "i";
    public static String INDEXES = "ix";
    public static String IS_JSON = "j";
    public static String IS_PREPARED = "is";
    public static String IS_SIMPLE_QUERY = "iq";
    public static String KEY = "k";
    public static String KV_VERSION = "kv";
    public static String LAST_INDEX = "li";
    public static String LIST_MAX_TO_READ = "lx";
    public static String LIST_START_INDEX = "ls";
    public static String MANUAL_COMMIT = "ma";
    public static String MATCH_VERSION = "mv";
    public static String MAX_EVENTS = "me";
    public static String MAX_POLL_INTERVAL = "mi";
    public static String MAX_QUERY_PARALLELISM = "mp";
    public static String MAX_READ_KB = "mr";
    public static String MAX_SHARD_USAGE_PERCENT = "ms";
    public static String MAX_WRITE_KB = "mw";
    public static String MODE = "mm";
    public static String NAME = "m";
    public static String NAMESPACE = "ns";
    public static String NUMBER_LIMIT = "nl";
    public static String NUM_OPERATIONS = "no";
    public static String NUM_QUERY_OPERATIONS = "nq";
    public static String OPERATION = "op";
    public static String OPERATIONS = "os";
    public static String OPERATION_ID = "od";
    public static String OP_CODE = "o";
    public static String PATH = "pt";
    public static String PAYLOAD = "p";
    public static String PREFER_THROTTLING = "pg";
    public static String PREPARE = "pp";
    public static String PREPARED_QUERY = "pq";
    public static String PREPARED_STATEMENT = "ps";
    public static String QUERY = "q";
    public static String QUERY_NAME = "qn";
    public static String QUERY_OPERATION_NUM = "on";
    public static String QUERY_VERSION = "qv";
    public static String RANGE = "rg";
    public static String RANGE_PATH = "rp";
    public static String READ_THROTTLE_COUNT = "rt";
    public static String READ_UNITS = "ru";
    public static String REGION = "rn";
    public static String RESOURCE = "ro";
    public static String RESOURCE_ID = "rd";
    public static String RETURN_ROW = "rr";
    public static String SERVER_MEMORY_CONSUMPTION = "sm";
    public static String SHARD_ID = "si";
    public static String START = "sr";
    public static String START_LOCATION = "so";
    public static String START_TIME = "se";
    public static String STATEMENT = "st";
    public static String STORAGE_THROTTLE_COUNT = "sl";
    public static String SYSTEM = "sy";
    public static String TABLES = "tb";
    public static String TABLE_DDL = "td";
    public static String TABLE_NAME = "n";
    public static String TABLE_OCID = "to";
    public static String TABLE_USAGE = "u";
    public static String TABLE_USAGE_PERIOD = "pd";
    public static String TIMEOUT = "t";
    public static String TOPO_SEQ_NUM = "ts";
    public static String TRACE_AT_LOG_FILES = "tf";
    public static String TRACE_LEVEL = "tl";
    public static String TTL = "tt";
    public static String TYPE = "y";
    public static String UPDATE_TTL = "ut";
    public static String VALUE = "l";
    public static String VERSION = "v";
    public static String VIRTUAL_SCAN = "vs";
    public static String VIRTUAL_SCANS = "vssa";
    public static String VIRTUAL_SCAN_SID = "vssid";
    public static String VIRTUAL_SCAN_PID = "vspid";
    public static String VIRTUAL_SCAN_NUM_TABLES = "vsnt";
    public static String VIRTUAL_SCAN_CURRENT_INDEX_RANGE = "vscir";
    public static String VIRTUAL_SCAN_PRIM_KEY = "vspk";
    public static String VIRTUAL_SCAN_SEC_KEY = "vssk";
    public static String VIRTUAL_SCAN_MOVE_AFTER = "vsma";
    public static String VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY = "vsjdrk";
    public static String VIRTUAL_SCAN_JOIN_PATH_TABLES = "vsjpt";
    public static String VIRTUAL_SCAN_JOIN_PATH_KEY = "vsjpk";
    public static String VIRTUAL_SCAN_JOIN_PATH_SEC_KEY = "vsjpsk";
    public static String VIRTUAL_SCAN_JOIN_PATH_MATCHED = "vsjpm";
    public static String WRITE_MULTIPLE = "wm";
    public static String WRITE_THROTTLE_COUNT = "wt";
    public static String WRITE_UNITS = "wu";

    /*
     * response fields
     */
    public static String ERROR_CODE = "e";
    public static String EXCEPTION = "x";
    public static String NUM_DELETIONS = "nd";
    public static String QUERY_BATCH_TRACES = "qts";
    public static String PROXY_TOPO_SEQNUM = "pn";
    public static String RETRY_HINT = "rh";
    public static String SHARD_IDS = "sa";
    public static String SUCCESS = "ss";
    public static String TOPOLOGY_INFO = "tp";
    public static String WM_FAILURE = "wf";
    public static String WM_FAIL_INDEX = "wi";
    public static String WM_FAIL_RESULT = "wr";
    public static String WM_SUCCESS = "ws";

    /* table metadata */
    public static String INITIALIZED = "it";
    public static String REPLICAS = "rc";
    public static String SCHEMA_FROZEN = "sf";
    public static String TABLE_SCHEMA = "ac";
    public static String TABLE_STATE = "as";

    /* system request */
    public static String SYSOP_RESULT = "rs";
    public static String SYSOP_STATE = "ta";

    /* throughput used and limits */
    public static String CONSUMED = "c";
    public static String LIMITS = "lm";
    public static String LIMITS_MODE = "mo";
    public static String READ_KB = "rk";
    public static String STORAGE_GB = "sg";
    public static String WRITE_KB = "wk";

    /* row metadata */
    public static String EXPIRATION = "xp";
    public static String CREATION_TIME = "ct";
    public static String MODIFIED = "md";
    public static String ROW = "r";
    public static String ROW_METADATA = "mt";
    public static String ROW_VERSION = "rv";

    /* operation metadata */
    public static String EXISTING_MOD_TIME = "em";
    public static String EXISTING_VALUE = "el";
    public static String EXISTING_VERSION = "ev";
    public static String EXISTING_ROW_METADATA = "ed";
    public static String GENERATED = "gn";
    public static String RETURN_INFO = "ri";

    /* query response fields */
    public static String DRIVER_QUERY_PLAN = "dq";
    public static String MATH_CONTEXT_CODE = "mc";
    public static String MATH_CONTEXT_ROUNDING_MODE = "rm";
    public static String MATH_CONTEXT_PRECISION = "cp";
    public static String NOT_TARGET_TABLES = "nt";
    public static String NUM_RESULTS = "nr";
    public static String QUERY_OPERATION = "qo";
    public static String QUERY_PLAN_STRING = "qs";
    public static String QUERY_RESULTS = "qr";
    public static String QUERY_RESULT_SCHEMA = "qc";
    public static String REACHED_LIMIT = "re";;
    public static String SORT_PHASE1_RESULTS = "p1";
    public static String TABLE_ACCESS_INFO = "ai";

    /* replica stats response fields */
    public static String NEXT_START_TIME = "ni";
    public static String REPLICA_STATS = "ra";
    public static String REPLICA_LAG = "rl";
    public static String TIME = "tm";

    private static String[][] mapVals = new String[][] {
        {ABORT_ON_FAIL,"ABORT_ON_FAIL"},
        {BATCH_COUNTER,"BATCH_COUNTER"},
        {BIND_VARIABLES,"BIND_VARIABLES"},
        {CDC_ENABLED,"CDC_ENABLED"},
        {COMPARTMENT_OCID,"COMPARTMENT_OCID"},
        {CONSISTENCY,"CONSISTENCY"},
        {CONSUMER_METADATA,"CONSUMER_METADATA"},
        {CONSUMER_TABLES,"CONSUMER_TABLES"},
        {CONTINUATION_KEY,"CONTINUATION_KEY"},
        {CURSOR,"CURSOR"},
        {DATA,"DATA"},
        {DEFINED_TAGS,"DEFINED_TAGS"},
        {DRL_OPTIN,"DRL_OPTIN"},
        {DURABILITY,"DURABILITY"},
        {END,"END"},
        {ETAG,"ETAG"},
        {EXACT_MATCH,"EXACT_MATCH"},
        {FIELDS,"FIELDS"},
        {FREE_FORM_TAGS,"FREE_FORM_TAGS"},
        {FORCE_RESET,"FORCE_RESET"},
        {GET_QUERY_PLAN,"GET_QUERY_PLAN"},
        {GET_QUERY_SCHEMA,"GET_QUERY_SCHEMA"},
        {GROUP_ID,"GROUP_ID"},
        {HEADER,"HEADER"},
        {IDEMPOTENT,"IDEMPOTENT"},
        {IDENTITY_CACHE_SIZE,"IDENTITY_CACHE_SIZE"},
        {INCLUSIVE,"INCLUSIVE"},
        {INDEX,"INDEX"},
        {INDEXES,"INDEXES"},
        {IS_JSON,"IS_JSON"},
        {IS_PREPARED,"IS_PREPARED"},
        {IS_SIMPLE_QUERY,"IS_SIMPLE_QUERY"},
        {KEY,"KEY"},
        {KV_VERSION,"KV_VERSION"},
        {LAST_INDEX,"LAST_INDEX"},
        {LIST_MAX_TO_READ,"LIST_MAX_TO_READ"},
        {LIST_START_INDEX,"LIST_START_INDEX"},
        {MANUAL_COMMIT,"MANUAL_COMMIT"},
        {MATCH_VERSION,"MATCH_VERSION"},
        {MAX_EVENTS,"MAX_EVENTS"},
        {MAX_POLL_INTERVAL,"MAX_POLL_INTERVAL"},
        {MAX_QUERY_PARALLELISM,"MAX_QUERY_PARALLELISM"},
        {MAX_READ_KB,"MAX_READ_KB"},
        {MAX_SHARD_USAGE_PERCENT,"MAX_SHARD_USAGE_PERCENT"},
        {MAX_WRITE_KB,"MAX_WRITE_KB"},
        {MODE,"MODE"},
        {NAME,"NAME"},
        {NAMESPACE,"NAMESPACE"},
        {NUMBER_LIMIT,"NUMBER_LIMIT"},
        {NUM_OPERATIONS,"NUM_OPERATIONS"},
        {NUM_QUERY_OPERATIONS,"NUM_QUERY_OPERATIONS"},
        {OPERATION,"OPERATION"},
        {OPERATIONS,"OPERATIONS"},
        {OPERATION_ID,"OPERATION_ID"},
        {OP_CODE,"OP_CODE"},
        {PATH,"PATH"},
        {PAYLOAD,"PAYLOAD"},
        {PREFER_THROTTLING,"PREFER_THROTTLING"},
        {PREPARE,"PREPARE"},
        {PREPARED_QUERY,"PREPARED_QUERY"},
        {PREPARED_STATEMENT,"PREPARED_STATEMENT"},
        {QUERY,"QUERY"},
        {QUERY_NAME,"QUERY_NAME"},
        {QUERY_OPERATION_NUM,"QUERY_OPERATION_NUM"},
        {QUERY_VERSION,"QUERY_VERSION"},
        {RANGE,"RANGE"},
        {RANGE_PATH,"RANGE_PATH"},
        {READ_THROTTLE_COUNT,"READ_THROTTLE_COUNT"},
        {READ_UNITS,"READ_UNITS"},
        {REGION,"REGION"},
        {RESOURCE,"RESOURCE"},
        {RESOURCE_ID,"RESOURCE_ID"},
        {RETURN_ROW,"RETURN_ROW"},
        {SERVER_MEMORY_CONSUMPTION,"SERVER_MEMORY_CONSUMPTION"},
        {SHARD_ID,"SHARD_ID"},
        {START,"START"},
        {START_LOCATION,"START_LOCATION"},
        {START_TIME,"START_TIME"},
        {STATEMENT,"STATEMENT"},
        {STORAGE_THROTTLE_COUNT,"STORAGE_THROTTLE_COUNT"},
        {SYSTEM,"SYSTEM"},
        {TABLES,"TABLES"},
        {TABLE_DDL,"TABLE_DDL"},
        {TABLE_NAME,"TABLE_NAME"},
        {TABLE_OCID,"TABLE_OCID"},
        {TABLE_USAGE,"TABLE_USAGE"},
        {TABLE_USAGE_PERIOD,"TABLE_USAGE_PERIOD"},
        {TIMEOUT,"TIMEOUT"},
        {TOPO_SEQ_NUM,"TOPO_SEQ_NUM"},
        {TRACE_AT_LOG_FILES,"TRACE_AT_LOG_FILES"},
        {TRACE_LEVEL,"TRACE_LEVEL"},
        {TTL,"TTL"},
        {TYPE,"TYPE"},
        {UPDATE_TTL,"UPDATE_TTL"},
        {VALUE,"VALUE"},
        {VERSION,"VERSION"},
        {VIRTUAL_SCAN,"VIRTUAL_SCAN"},
        {VIRTUAL_SCANS,"VIRTUAL_SCANS"},
        {VIRTUAL_SCAN_SID,"VIRTUAL_SCAN_SID"},
        {VIRTUAL_SCAN_PID,"VIRTUAL_SCAN_PID"},
        {VIRTUAL_SCAN_NUM_TABLES,"VIRTUAL_SCAN_NUM_TABLES"},
        {VIRTUAL_SCAN_CURRENT_INDEX_RANGE,"VIRTUAL_SCAN_CURRENT_INDEX_RANGE"},
        {VIRTUAL_SCAN_PRIM_KEY,"VIRTUAL_SCAN_PRIM_KEY"},
        {VIRTUAL_SCAN_SEC_KEY,"VIRTUAL_SCAN_SEC_KEY"},
        {VIRTUAL_SCAN_MOVE_AFTER,"VIRTUAL_SCAN_MOVE_AFTER"},
        {VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY,"VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY"},
        {VIRTUAL_SCAN_JOIN_PATH_TABLES,"VIRTUAL_SCAN_JOIN_PATH_TABLES"},
        {VIRTUAL_SCAN_JOIN_PATH_KEY,"VIRTUAL_SCAN_JOIN_PATH_KEY"},
        {VIRTUAL_SCAN_JOIN_PATH_SEC_KEY,"VIRTUAL_SCAN_JOIN_PATH_SEC_KEY"},
        {VIRTUAL_SCAN_JOIN_PATH_MATCHED,"VIRTUAL_SCAN_JOIN_PATH_MATCHED"},
        {WRITE_MULTIPLE,"WRITE_MULTIPLE"},
        {WRITE_THROTTLE_COUNT,"WRITE_THROTTLE_COUNT"},
        {WRITE_UNITS,"WRITE_UNITS"},
        {ERROR_CODE,"ERROR_CODE"},
        {EXCEPTION,"EXCEPTION"},
        {NUM_DELETIONS,"NUM_DELETIONS"},
        {QUERY_BATCH_TRACES,"QUERY_BATCH_TRACES"},
        {PROXY_TOPO_SEQNUM,"PROXY_TOPO_SEQNUM"},
        {RETRY_HINT,"RETRY_HINT"},
        {SHARD_IDS,"SHARD_IDS"},
        {SUCCESS,"SUCCESS"},
        {TOPOLOGY_INFO,"TOPOLOGY_INFO"},
        {WM_FAILURE,"WM_FAILURE"},
        {WM_FAIL_INDEX,"WM_FAIL_INDEX"},
        {WM_FAIL_RESULT,"WM_FAIL_RESULT"},
        {WM_SUCCESS,"WM_SUCCESS"},
        {INITIALIZED,"INITIALIZED"},
        {REPLICAS,"REPLICAS"},
        {SCHEMA_FROZEN,"SCHEMA_FROZEN"},
        {TABLE_SCHEMA,"TABLE_SCHEMA"},
        {TABLE_STATE,"TABLE_STATE"},
        {SYSOP_RESULT,"SYSOP_RESULT"},
        {SYSOP_STATE,"SYSOP_STATE"},
        {CONSUMED,"CONSUMED"},
        {LIMITS,"LIMITS"},
        {LIMITS_MODE,"LIMITS_MODE"},
        {READ_KB,"READ_KB"},
        {STORAGE_GB,"STORAGE_GB"},
        {WRITE_KB,"WRITE_KB"},
        {EXPIRATION,"EXPIRATION"},
        {CREATION_TIME,"CREATION_TIME"},
        {MODIFIED,"MODIFIED"},
        {ROW,"ROW"},
        {ROW_METADATA,"ROW_METADATA"},
        {ROW_VERSION,"ROW_VERSION"},
        {EXISTING_MOD_TIME,"EXISTING_MOD_TIME"},
        {EXISTING_VALUE,"EXISTING_VALUE"},
        {EXISTING_VERSION,"EXISTING_VERSION"},
        {EXISTING_ROW_METADATA,"EXISTING_ROW_METADATA"},
        {GENERATED,"GENERATED"},
        {RETURN_INFO,"RETURN_INFO"},
        {DRIVER_QUERY_PLAN,"DRIVER_QUERY_PLAN"},
        {MATH_CONTEXT_CODE,"MATH_CONTEXT_CODE"},
        {MATH_CONTEXT_ROUNDING_MODE,"MATH_CONTEXT_ROUNDING_MODE"},
        {MATH_CONTEXT_PRECISION,"MATH_CONTEXT_PRECISION"},
        {NOT_TARGET_TABLES,"NOT_TARGET_TABLES"},
        {NUM_RESULTS,"NUM_RESULTS"},
        {QUERY_OPERATION,"QUERY_OPERATION"},
        {QUERY_PLAN_STRING,"QUERY_PLAN_STRING"},
        {QUERY_RESULTS,"QUERY_RESULTS"},
        {QUERY_RESULT_SCHEMA,"QUERY_RESULT_SCHEMA"},
        {REACHED_LIMIT,"REACHED_LIMIT"},
        {SORT_PHASE1_RESULTS,"SORT_PHASE1_RESULTS"},
        {TABLE_ACCESS_INFO,"TABLE_ACCESS_INFO"},
        {NEXT_START_TIME,"NEXT_START_TIME"},
        {REPLICA_STATS,"REPLICA_STATS"},
        {REPLICA_LAG,"REPLICA_LAG"},
        {TIME,"TIME"},
    };

    private static HashMap<String, String> fieldMap = null;

    public static String readable(String field) {
        if (fieldMap == null) {
            fieldMap = new HashMap<String, String>();
            for (int x=0; x<mapVals.length; x++) {
                fieldMap.put(mapVals[x][0], mapVals[x][1]);
            }
        }
        String val = fieldMap.get(field);
        if (val == null) {
            return field;
        }
        return val;
    }
}
