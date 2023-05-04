/*-
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates. All rights reserved.
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
    public static String BIND_VARIABLES = "bv";
    public static String COMPARTMENT_OCID = "cc";
    public static String CONSISTENCY = "co";
    public static String CONTINUATION_KEY = "ck";
    public static String DATA = "d";
    public static String DEFINED_TAGS = "dt";
    public static String DURABILITY = "du";
    public static String END = "en";
    public static String ETAG = "et";
    public static String EXACT_MATCH = "ec";
    public static String FIELDS = "f";
    public static String FREE_FORM_TAGS = "ff";
    public static String GET_QUERY_PLAN = "gq";
    public static String GET_QUERY_SCHEMA = "gs";
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
    public static String MATCH_VERSION = "mv";
    public static String MAX_READ_KB = "mr";
    public static String MAX_SHARD_USAGE_PERCENT = "ms";
    public static String MAX_WRITE_KB = "mw";
    public static String NAME = "m";
    public static String NAMESPACE = "ns";
    public static String NUMBER_LIMIT = "nl";
    public static String NUM_OPERATIONS = "no";
    public static String OPERATIONS = "os";
    public static String OPERATION_ID = "od";
    public static String OP_CODE = "o";
    public static String PATH = "pt";
    public static String PAYLOAD = "p";
    public static String PREPARE = "pp";
    public static String PREPARED_QUERY = "pq";
    public static String PREPARED_STATEMENT = "ps";
    public static String QUERY = "q";
    public static String QUERY_VERSION = "qv";
    public static String RANGE = "rg";
    public static String RANGE_PATH = "rp";
    public static String READ_THROTTLE_COUNT = "rt";
    public static String RETURN_ROW = "rr";
    public static String SHARD_ID = "si";
    public static String SERVER_MEMORY_CONSUMPTION = "sm";
    public static String START = "sr";
    public static String STATEMENT = "st";
    public static String STORAGE_THROTTLE_COUNT = "sl";
    public static String TABLES = "tb";
    public static String TABLE_DDL = "td";
    public static String TABLE_NAME = "n";
    public static String TABLE_OCID = "to";
    public static String TABLE_USAGE = "u";
    public static String TABLE_USAGE_PERIOD = "pd";
    public static String TIMEOUT = "t";
    public static String TOPO_SEQ_NUM = "ts";
    public static String TRACE_LEVEL = "tl";
    public static String TTL = "tt";
    public static String TYPE = "y";
    public static String UPDATE_TTL = "ut";
    public static String VALUE = "l";
    public static String VERSION = "v";
    public static String WRITE_MULTIPLE = "wm";
    public static String WRITE_THROTTLE_COUNT = "wt";

    /*
     * response fields
     */
    public static String ERROR_CODE = "e";
    public static String EXCEPTION = "x";
    public static String NUM_DELETIONS = "nd";
    public static String RETRY_HINT = "rh";
    public static String SUCCESS = "ss";
    public static String WM_FAILURE = "wf";
    public static String WM_FAIL_INDEX = "wi";
    public static String WM_FAIL_RESULT = "wr";
    public static String WM_SUCCESS = "ws";

    /* table metadata */
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
    public static String READ_UNITS = "ru";
    public static String STORAGE_GB = "sg";
    public static String WRITE_KB = "wk";
    public static String WRITE_UNITS = "wu";

    /* row metadata */
    public static String EXPIRATION = "xp";
    public static String MODIFIED = "md";
    public static String ROW = "r";
    public static String ROW_VERSION = "rv";

    /* operation metadata */
    public static String EXISTING_MOD_TIME = "em";
    public static String EXISTING_VALUE = "el";
    public static String EXISTING_VERSION = "ev";
    public static String GENERATED = "gn";
    public static String RETURN_INFO = "ri";

    /* query response fields */
    public static String DRIVER_QUERY_PLAN = "dq";
    public static String MATH_CONTEXT_CODE = "mc";
    public static String MATH_CONTEXT_ROUNDING_MODE = "rm";
    public static String MATH_CONTEXT_PRECISION = "cp";
    public static String NOT_TARGET_TABLES = "nt";
    public static String NUM_RESULTS = "nr";
    public static String PROXY_TOPO_SEQNUM = "pn";
    public static String QUERY_OPERATION = "qo";
    public static String QUERY_PLAN_STRING = "qs";
    public static String QUERY_RESULTS = "qr";
    public static String QUERY_RESULT_SCHEMA = "qc";
    public static String REACHED_LIMIT = "re";
    public static String SHARD_IDS = "sa";
    public static String SORT_PHASE1_RESULTS = "p1";
    public static String TABLE_ACCESS_INFO = "ai";
    public static String TOPOLOGY_INFO = "tp";

    private static String[][] mapVals = new String[][] {
        {ABORT_ON_FAIL,"ABORT_ON_FAIL"},
        {BIND_VARIABLES,"BIND_VARIABLES"},
        {COMPARTMENT_OCID,"COMPARTMENT_OCID"},
        {CONSISTENCY,"CONSISTENCY"},
        {CONTINUATION_KEY,"CONTINUATION_KEY"},
        {DATA,"DATA"},
        {DEFINED_TAGS,"DEFINED_TAGS"},
        {DURABILITY,"DURABILITY"},
        {END,"END"},
        {ETAG,"ETAG"},
        {EXACT_MATCH,"EXACT_MATCH"},
        {FIELDS,"FIELDS"},
        {FREE_FORM_TAGS,"FREE_FORM_TAGS"},
        {GET_QUERY_PLAN,"GET_QUERY_PLAN"},
        {GET_QUERY_SCHEMA,"GET_QUERY_SCHEMA"},
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
        {MATCH_VERSION,"MATCH_VERSION"},
        {MAX_READ_KB,"MAX_READ_KB"},
        {MAX_SHARD_USAGE_PERCENT,"MAX_SHARD_USAGE_PERCENT"},
        {MAX_WRITE_KB,"MAX_WRITE_KB"},
        {NAME,"NAME"},
        {NAMESPACE,"NAMESPACE"},
        {NUMBER_LIMIT,"NUMBER_LIMIT"},
        {NUM_OPERATIONS,"NUM_OPERATIONS"},
        {OPERATIONS,"OPERATIONS"},
        {OPERATION_ID,"OPERATION_ID"},
        {OP_CODE,"OP_CODE"},
        {PATH,"PATH"},
        {PAYLOAD,"PAYLOAD"},
        {PREPARE,"PREPARE"},
        {PREPARED_QUERY,"PREPARED_QUERY"},
        {PREPARED_STATEMENT,"PREPARED_STATEMENT"},
        {QUERY,"QUERY"},
        {QUERY_VERSION,"QUERY_VERSION"},
        {RANGE,"RANGE"},
        {RANGE_PATH,"RANGE_PATH"},
        {READ_THROTTLE_COUNT,"READ_THROTTLE_COUNT"},
        {RETURN_ROW,"RETURN_ROW"},
        {SHARD_ID,"SHARD_ID"},
        {SERVER_MEMORY_CONSUMPTION,"SERVER_MEMORY_CONSUMPTION"},
        {START,"START"},
        {STATEMENT,"STATEMENT"},
        {STORAGE_THROTTLE_COUNT,"STORAGE_THROTTLE_COUNT"},
        {TABLES,"TABLES"},
        {TABLE_DDL,"TABLE_DDL"},
        {TABLE_NAME,"TABLE_NAME"},
        {TABLE_OCID,"TABLE_OCID"},
        {TABLE_USAGE,"TABLE_USAGE"},
        {TABLE_USAGE_PERIOD,"TABLE_USAGE_PERIOD"},
        {TIMEOUT,"TIMEOUT"},
        {TOPO_SEQ_NUM,"TOPO_SEQ_NUM"},
        {TRACE_LEVEL,"TRACE_LEVEL"},
        {TTL,"TTL"},
        {TYPE,"TYPE"},
        {UPDATE_TTL,"UPDATE_TTL"},
        {VALUE,"VALUE"},
        {VERSION,"VERSION"},
        {WRITE_MULTIPLE,"WRITE_MULTIPLE"},
        {WRITE_THROTTLE_COUNT,"WRITE_THROTTLE_COUNT"},
        {ERROR_CODE,"ERROR_CODE"},
        {EXCEPTION,"EXCEPTION"},
        {NUM_DELETIONS,"NUM_DELETIONS"},
        {RETRY_HINT,"RETRY_HINT"},
        {SUCCESS,"SUCCESS"},
        {WM_FAILURE,"WM_FAILURE"},
        {WM_FAIL_INDEX,"WM_FAIL_INDEX"},
        {WM_FAIL_RESULT,"WM_FAIL_RESULT"},
        {WM_SUCCESS,"WM_SUCCESS"},
        {TABLE_SCHEMA,"TABLE_SCHEMA"},
        {TABLE_STATE,"TABLE_STATE"},
        {SYSOP_RESULT,"SYSOP_RESULT"},
        {SYSOP_STATE,"SYSOP_STATE"},
        {CONSUMED,"CONSUMED"},
        {LIMITS,"LIMITS"},
        {LIMITS_MODE,"LIMITS_MODE"},
        {READ_KB,"READ_KB"},
        {READ_UNITS,"READ_UNITS"},
        {STORAGE_GB,"STORAGE_GB"},
        {WRITE_KB,"WRITE_KB"},
        {WRITE_UNITS,"WRITE_UNITS"},
        {EXPIRATION,"EXPIRATION"},
        {MODIFIED,"MODIFIED"},
        {ROW,"ROW"},
        {ROW_VERSION,"ROW_VERSION"},
        {EXISTING_MOD_TIME,"EXISTING_MOD_TIME"},
        {EXISTING_VALUE,"EXISTING_VALUE"},
        {EXISTING_VERSION,"EXISTING_VERSION"},
        {GENERATED,"GENERATED"},
        {RETURN_INFO,"RETURN_INFO"},
        {DRIVER_QUERY_PLAN,"DRIVER_QUERY_PLAN"},
        {MATH_CONTEXT_CODE,"MATH_CONTEXT_CODE"},
        {MATH_CONTEXT_ROUNDING_MODE,"MATH_CONTEXT_ROUNDING_MODE"},
        {MATH_CONTEXT_PRECISION,"MATH_CONTEXT_PRECISION"},
        {NOT_TARGET_TABLES,"NOT_TARGET_TABLES"},
        {NUM_RESULTS,"NUM_RESULTS"},
        {PROXY_TOPO_SEQNUM,"PROXY_TOPO_SEQNUM"},
        {QUERY_OPERATION,"QUERY_OPERATION"},
        {QUERY_PLAN_STRING,"QUERY_PLAN_STRING"},
        {QUERY_RESULTS,"QUERY_RESULTS"},
        {QUERY_RESULT_SCHEMA,"QUERY_RESULT_SCHEMA"},
        {REACHED_LIMIT,"REACHED_LIMIT"},
        {SHARD_IDS,"SHARD_IDS"},
        {SORT_PHASE1_RESULTS,"SORT_PHASE1_RESULTS"},
        {TABLE_ACCESS_INFO,"TABLE_ACCESS_INFO"},
        {TOPOLOGY_INFO,"TOPOLOGY_INFO"},
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
