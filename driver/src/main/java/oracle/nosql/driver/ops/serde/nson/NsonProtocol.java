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
    public static String HEADER = "header";
    public static String PAYLOAD = "payload";
    public static String VERSION = "version";
    public static String OP = "op";
    public static String TIMEOUT = "timeout";
    public static String TABLE_NAME = "table_name";
    public static String CONSISTENCY = "consistency";
    public static String DURABILITY = "durability";
    public static String MAX_READ_KB = "max_read_kb";
    public static String MAX_WRITE_KB = "max_write_kb";
    public static String DATA = "data";
    public static String TTL = "ttl";
    public static String UPDATE_TTL = "update_ttl";
    public static String KV_VERSION = "kv_version";
    public static String MATCH_VERSION = "match_version";
    public static String EXACT_MATCH = "exact_match";
    public static String IDEMPOTENT = "idempotent";
    public static String RETURN_ROW = "return_row";
    public static String KEY = "key";
    public static String VALUE = "value";
    public static String NAME = "name";
    public static String IS_JSON = "is_json";
    public static String QUERY = "query";
    public static String PREPARE = "prepare";
    public static String STATEMENT = "statement";
    public static String PREPARED_STATEMENT = "prepared_statement";
    public static String WRITE_MULTIPLE = "write_multiple";
    public static String ABORT_ON_FAIL = "abort_on_fail";
    public static String RANGE = "range";
    public static String RANGE_PATH = "range_path";
    public static String START = "start";
    public static String END = "end";
    public static String INCLUSIVE = "inclusive";
    public static String COMPARTMENT_OCID = "compartment_ocid";
    public static String TABLE_OCID = "table_ocid";
    public static String NAMESPACE = "namespace";
    public static String OPERATION_ID = "operation_id";
    public static String OPERATIONS = "operations";
    public static String NUM_OPERATIONS = "num_operations";
    public static String IDENTITY_CACHE_SIZE = "id_cache_size";
    public static String TOPO_SEQ_NUM = "topo_seq";
    public static String SHARD_ID = "shard_id";
    public static String NUMBER_LIMIT = "number_limit";
    public static String IS_PREPARED = "is_prepared";
    public static String QUERY_VERSION = "query_ver";
    public static String TRACE_LEVEL = "trace_level";
    public static String PREPARED_QUERY = "prepared_query";
    public static String IS_SIMPLE_QUERY = "is_simple";
    public static String CONTINUATION_KEY = "cont_key";
    public static String BIND_VARIABLES = "bind_vars";

    /*
     * response fields
     */
    public static String ERROR_CODE = "error_code";
    public static String EXCEPTION = "exception";
    public static String RETRY_HINT = "retry_hint";
    public static String SUCCESS = "success";
    public static String WM_SUCCESS = "wm_success";
    public static String WM_FAILURE = "wm_failure";
    public static String WM_FAIL_INDEX = "wm_fail_index";
    public static String WM_FAIL_RESULT = "wm_fail_result";
    public static String NUM_DELETIONS = "num_deletions";

    // table metadata
    public static String TABLE_STATE = "table_state";
    public static String TABLE_SCHEMA = "table_schema";

    // throughput used and limits
    public static String LIMITS = "limits";
    public static String LIMITS_MODE = "mode";
    public static String CONSUMED = "consumed";
    public static String READ_UNITS = "read_units";
    public static String WRITE_UNITS = "write_units";
    public static String READ_KB = "read_kb";
    public static String WRITE_KB = "write_kb";
    public static String STORAGE_GB = "storage_gb";
    // row metadata
    public static String EXPIRATION = "expiration";
    public static String MODIFIED = "modified";
    public static String ROW_VERSION = "row_version";
    public static String ROW = "row";
    // operation metadata
    public static String GENERATED = "generated";
    public static String RETURN_INFO = "return_info";
    public static String EXISTING_MOD_TIME = "existing_mod_time";
    public static String EXISTING_VERSION = "existing_version";
    public static String EXISTING_VALUE = "existing_value";
    // RETURN_ROW defined above

    /* query response fields */
    public static String QUERY_RESULTS = "query_results";
    public static String QUERY_PLAN_STRING = "query_planstr";
    public static String SORT_PHASE1_RESULTS = "sp1_results";
    public static String NUM_RESULTS = "num_results";
    public static String DRIVER_QUERY_PLAN = "dq_plan";
    public static String REACHED_LIMIT = "reached_limit";
    public static String TOPOLOGY_INFO = "topo_info";
    public static String TABLE_ACCESS_INFO = "access_info";
    public static String NOT_TARGET_TABLES = "not_tables";
    public static String QUERY_OPERATION = "query_op";
    public static String PROXY_TOPO_SEQNUM = "ptopo_seqnum";
    public static String SHARD_IDS = "shard_ids";
    public static String MATH_CONTEXT_CODE = "mc_c";
    public static String MATH_CONTEXT_PRECISION = "mc_p";
    public static String MATH_CONTEXT_ROUNDING_MODE = "mc_rm";
}
