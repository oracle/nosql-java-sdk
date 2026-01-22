/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryPaginatorResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.util.ConcurrentUtil;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.StringValue;

import org.junit.Assert;
import org.junit.Test;
import reactor.adapter.JdkFlowAdapter;

/**
 * Test queries in async mode
 */
public class AsyncQueryTest extends ProxyTestBase {

    private static final boolean showResults = Boolean.getBoolean("test.showresults");
    private static final int traceLevel = Integer.getInteger("test.tracelevel", 0);

    private final static int MIN_QUERY_COST = 2;

    final static String tableName = "testTable";
    final static String indexName = "idxName";
    final static String jsonTable = "jsonTable";
    /* timeout for all table operations */
    final static int timeout = 20000;

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS testTable (" +
        "sid INTEGER, " +
        "id INTEGER, " +
        "name STRING, " +
        "age INTEGER, " +
        "state STRING, " +
        "salary LONG, " +
        "array ARRAY(INTEGER), " +
        "longString STRING," +
        "PRIMARY KEY(SHARD(sid), id))";

    final boolean multishard = false; /* TBD */

    /* Create an index on testTable(name) */
    final String createIdxNameDDL =
        "CREATE INDEX IF NOT EXISTS idxName on testTable(name)";

    /* Create an index on testTable(sid, age)*/
    final String createIdxSidAgeDDL =
        "CREATE INDEX IF NOT EXISTS idxSidAge ON testTable(sid, age)";

    /* Create an index on testTable(state, age)*/
    final String createIdxStateAgeDDL =
        "CREATE INDEX IF NOT EXISTS idxStateAge ON testTable(state, age)";

   /* Create an index on testTable(state, age)*/
    final String createIdxArrayDDL =
        "CREATE INDEX IF NOT EXISTS idxArray ON testTable(array[])";

    /* Create a table with Json field */
    final static String createJsonTableDDL =
        "CREATE TABLE IF NOT EXISTS jsonTable (id INTEGER, info JSON, " +
        "PRIMARY KEY(id))";

    /* Create a table with 2 major keys, used in testIllegalQuery() */
    final static String createTestTableDDL =
        "CREATE TABLE IF NOT EXISTS test (" +
            "sid1 INTEGER, " +
            "sid2 INTEGER, " +
            "id INTEGER, " +
            "name STRING, " +
            "PRIMARY KEY(SHARD(sid1, sid2), id))";

    final static String createIdxSid1NameDDL =
        "CREATE INDEX IF NOT EXISTS idxSid1Name ON test(sid1, name)";

    final static String createIdxNameSid1Sid2DDL =
        "CREATE INDEX IF NOT EXISTS idxNameSid1Sid2 ON test(name, sid1, sid2)";

    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();

        tableOperationAsync(asyncHandle, createTableDDL,
                       new TableLimits(45000, 15000, 50)).join();

        tableOperationAsync(asyncHandle, createIdxNameDDL, null).join();
    }

    @Override
    public void afterTest() throws Exception {
        tableOperationAsync(asyncHandle, "DROP TABLE IF EXISTS " + tableName,
                null).join();
        super.afterTest();
    }

    @Test
    public void testQuery() {

        final String fullQuery = "select * from testTable";
        final String predQuery = "select * from testTable where sid > 7";
        final String updateQuery =
            "update testTable f set f.name = 'joe' where sid = 9 and id = 9 ";
        final String getQuery =
            "select name from testTable where sid = 9 and id = 9 ";
        final String queryWithVariables =
            "declare $sid integer; $id integer;" +
            "select name from testTable where sid = $sid and id >= $id";
        final String queryWithSort =
            "select * from testTable where sid = 0 order by sid, id";

        final int numMajor = 10;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, 1);

        /*
         * Perform a simple query
         */
        executeQuery(predQuery, null, 20, 0, false);

        /*
         * Perform an update query
         */
        try (QueryRequest queryRequest = newQueryRequest()) {
            queryRequest.setStatement(updateQuery);
            asyncHandle.query(queryRequest).join();
        }

        /*
         * Use a simple get query to validate the update
         */
        try (QueryRequest queryRequest = newQueryRequest()) {
            queryRequest.setStatement(getQuery);
            QueryResult queryRes = asyncHandle.query(queryRequest).join();
            assertEquals(1, queryRes.getResults().size());
            assertEquals("joe",
                queryRes.getResults().get(0).get("name").getString());

            /* full scan to count rows */
            executeQuery(fullQuery, null, numRows, 0, false /* usePrepStmt */);
            executeQuery(fullQuery, null, numRows, 0, true /* usePrepStmt */);
        }
        /*
         * Query with external variables
         */
        Map<String, FieldValue> variables = new HashMap<>();
        variables.put("$sid", new IntegerValue(9));
        variables.put("$id", new StringValue("3"));
        executeQuery(queryWithVariables, variables, 7, 0, true);

        /* Query with sort */
        executeQuery(queryWithSort, null, numPerMajor, 0,
                     false /* usePrepStmt */);
        executeQuery(queryWithSort, null, numPerMajor, 0,
                     true /* usePrepStmt */);
    }

    /**
     * Test query with numeric-base and size-based limits
     */
    @Test
    public void testLimits() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * number-based limit
         */

        /* Read rows from all partitions with number-based limit. */
        String query = "select * from testTable";
        int expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                     numRows /* numReadRows */,
                                     numRows /* numReadKeys */);
        int expCnt = numRows;
        int[] limits = new int[] {0, 20, 100, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, limit, 0, recordKB);
        }

        /* Read rows from single partition with number-based limit. */
        query = "select * from testTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        limits = new int[] {0, 20, 100, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0, recordKB);
        }

        /* Read rows from all shards with number-based limit. */
        query = "select * from testTable where name = 'name_1'";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */, recordKB);
        }

        /*
         * Size-based limit
         */

        /* Read rows from all partitions with size limit. */
        query = "select * from testTable";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        int[] maxReadKBs = new int[] {0, 500, 1000, 2000};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Read rows from single partition with size limit. */
        query = "select * from testTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        maxReadKBs = new int[] {0, 50, 100, 250};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Read rows from all shards with size limit. */
        query = "select * from testTable where name = \"name_1\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        maxReadKBs = new int[] {0, 5, 10, 25};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Number-based and size-based limit
         */

        /* Read rows from all partitions with number and size limit. */
        query = "select * from testTable";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        executeQuery(query, false /* keyOnly */, false/* indexScan */, expCnt,
                     expReadKB, 50 /* numLimit */, 100 /* sizeLimit */,
                     recordKB);

        /* Read rows from single partition with number and size limit. */
        query = "select * from testTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        executeQuery(query, false /* keyOnly */, false/* indexScan */, expCnt,
                     expReadKB, 10 /* numLimit */, 20 /* sizeLimit */, recordKB);

        /* Read rows from all shards with number and size limit. */
        query = "select * from testTable where name = \"name_1\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        executeQuery(query, false /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 5 /* numLimit */, 10 /* sizeLimit */,
                     recordKB);
    }

    @Test
    public void testDupElim() {
        final int numMajor = 10;
        final int numPerMajor = 40;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperationAsync(asyncHandle, createIdxArrayDDL, null).join();

        String query =
            "select sid, id, t.array[size($)-2:] " +
            "from testTable t " +
            "where t.array[] >any 11";

        /* Prepare first, then execute */
        executeQuery(query, null, 200, 20, true);
    }

    @Test
    public void testOrderByPartitions() {
        final int numMajor = 5;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperationAsync(asyncHandle, createIdxStateAgeDDL, null).join();

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        /*
         * Case 1: partial key
         */
        query = "select sid, id, name, state " +
                "from testTable " +
                "order by sid ";

        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        maxReadKBs = new int[] {0, 4, 25, 37, 66};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }

        /*
         * Case 2: partial key offset limit
         */
       query = "select sid, id, name, state " +
                "from testTable " +
                "order by sid " +
                "limit 10 offset 4";

        expCnt = 10;
        maxReadKBs = new int[] {0, 5, 6, 7, 8, 9, 20, 44, 81};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, -1 /*expReadKB*/, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }

        /*
         * Case 3: partial key offset limit
         */
        query = "select sid, id, name, state " +
                "from testTable " +
                "order by sid " +
                "limit 5 offset 44";

        expCnt = 5;
        maxReadKBs = new int[] {0, 5, 14, 51, 88};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, -1 /*expReadKB*/, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }
    }

    @Test
    public void testGroupByPartitions() {

        final int numMajor = 5;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperationAsync(asyncHandle, createIdxStateAgeDDL, null).join();

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;


        /*
         * Case 1
         */
        query = "select sid, count(*) as cnt, sum(salary) as sum " +
                "from testTable " +
                "group by sid";

        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 5;
        /* maxReadKBs = new int[] {0, 4, 25, 37, 66}; */
        maxReadKBs = new int[] {0};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }
    }

    @Test
    public void testOrderByShards() {

        final int numMajor = 10;
        final int numPerMajor = 40;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperationAsync(asyncHandle, createIdxStateAgeDDL, null).join();

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        /*
         * Case 1: multi-shard, covering index
         */
        query = "select sid, id, state " +
                "from testTable " +
                "order by state " +
                "limit 20 offset 4";

        if (multishard) {
            /*
             * readKBs are not deterministic with multishard
             * See KVSTORE-649
             */
            expReadKB = -1;
        } else {
            expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                     0 /* numReadRows */,
                                     24 /* numReadKeys */);
        }

        expCnt = 20;
        maxReadKBs = new int[] {0, 5, 7, 11};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, true /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }

        /*
         * Case 2: multi-shard, non-covering index
         */
        query = "select sid, id, state, salary " +
                "from testTable " +
                "order by state " +
                "limit 27 offset 5";

        if (multishard) {
            /*
             * readKBs are not deterministic with multishard
             * See KVSTORE-649
             */
            expReadKB = -1;
        } else {
            expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                     32 /* numReadRows */,
                                     32 /* numReadKeys */);
        }

        expCnt = 27;
        maxReadKBs = new int[] {6, 7, 8};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }

        /*
         * Case 3: single-partition, non-covering index
         */
        query = "select sid, id, state, salary " +
                "from testTable " +
                "where sid = 3 " +
                "order by sid, id " +
                "limit 27 offset 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 32 /* numReadRows */,
                                 32 /* numReadKeys */);
        expCnt = 27;
        maxReadKBs = new int[] {4, 5, 12};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL, false, null);
        }
    }

    @Test
    public void testGroupByShards() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        tableOperationAsync(asyncHandle, createIdxStateAgeDDL, null).join();
        /*
         * Case 1.
         */
        query = "select count(*) from testTable where state = \"CA\"";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 210);
        expCnt = 1;
        /* size-based limit */
        maxReadKBs = new int[] {10, 17, 23, 37, 209, 210, 500};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Case 2.
         * sum(salary) = 165000
         */
        query = "select count(*), sum(salary) from testTable " +
                "where state = \"VT\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 200 /* numReadRows */,
                                 200 /* numReadKeys */);
        expCnt = 1;
        /* size-based limit */
        maxReadKBs = new int[] {9, 19, 31, 44, 200, 500};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Prepare first, then execute */
        executeQuery(query, null, 1, 22, true);

        /*
         * Case 3.
         */
        query = "select state, count(*) from testTable group by state";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 1010);
        expCnt = 5;
        /* size-based limit */
        maxReadKBs = new int[] {30};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Case 4.
         */
        query =
            "select state, "              +
            "       count(*) as cnt, "    +
            "       sum(salary) as sum, " +
            "       avg(salary) as avg "  +
            "from testTable "+
            "group by state";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 1010 /* numReadRows */,
                                 1010);
        expCnt = 5;
        /* size-based limit */
        maxReadKBs = new int[] {34};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

    }

    /**
     * Test group-by query with numeric-base limit and/or size-based limits
     *
     *  1. Single partition scan, key-only
     *      select count(*) from testTable where sid = 1
     *
     *  2. Single partition scan, key + row
     *      select min(name), min(age) from testTable where sid = 1
     *
     *  3. All partitions scan, key only
     *      select count(*) from testTable group by sid
     *
     *  4. All partitions scan, key + row
     *      select min(name) from testTable group by sid
     *
     *  5. All shards scan, key only
     *      select count(*) from testTable group by sid, name
     *
     *  6. All shards scan, key + row
     *      select max(name) from testTable group by sid, name
     *
     *  7. All partitions scan, key only, single row returned.
     *      select count(*) from testTable
     *
     *  8. All shards scan, key only, single row returned.
     *      select min(name) from testTable
     */
    @Test
    public void testGroupByWithLimits() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        String query;
        int expReadKB, expCnt;
        int[] limits, maxReadKBs;

        tableOperationAsync(asyncHandle, createIdxSidAgeDDL, null).join();

        /*
         * Case: Single partition scan, key only
         */
        query = "select count(*) from testTable where sid = 1";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = 1;
        /* number-based limit */
        limits = new int[] {0, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 50, 100, 101};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based and size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 50 /* maxReadKB */,
                     recordKB);

        /*
         * Case 2: Single partition scan, key + row
         */
        query = "select min(salary), min(age) from testTable where sid = 1";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = 1;
        /* number-based limit */
        limits = new int[] {0, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 300, 303};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);

        /*
         * Case 3: All partitions scan, key only
         */
        query = "select count(*) from testTable group by sid";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor;
        /* number-based limit */
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 2 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 5 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);

        /*
         * Case 4: All partitions scan, key + row
         */
        query = "select min(salary) from testTable group by sid";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor;
        /* number-based limit */
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 2047};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 400 /* maxReadKB */,
                     recordKB);
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 3 /* numLimit */, 400 /* maxReadKB */,
                     recordKB);

        /*
         * Case 5: All shards can, key only
         */
        query = "select count(*) from testTable group by sid, age";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor * 10;

        /* number-based limit */
        limits = new int[] {0, 5, 50, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* number-based and size-based limit */
        executeQuery(query, true /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 10 /* numLimit */, 100, recordKB);

        /*
         * Case 6: All shards can, key + row
         */
        query = "select max(salary) from testTable group by sid, age";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor * 10;

        /* number-based limit */
        limits = new int[] {0, 5, 50, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }

        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 2047};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        executeQuery(query, false /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 10 /* numLimit */, 300 /* maxReadKB */,
                     recordKB);

        /*
         * Case 7: All partitions scan, key only. Single row returned.
         */
        query = "select count(*) from testTable";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 1;
        /* number-based limits */
        limits = new int[] {0, 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010 };
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 500 /* maxReadKB */,
                     recordKB);

        /*
         * Case 8: All shards scan, key only. Single row returned.
         */
        query = "select min(name) from testTable";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 1;
        /* number-based limits */
        limits = new int[] {0, 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010 };
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 500 /* maxReadKB */,
                     recordKB);
    }

    @Test
    public void testDelete() {
        final int numMajor = 5;
        final int numPerMajor = 100;
        final int recordKB = 4;

        tableOperationAsync(asyncHandle, createIdxStateAgeDDL, null).join();

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        int expReadKB, expCnt;
        int[] maxReadKBs;
        String query;

        /*
         * Case 1. ALL_SHARDS delete, without RETURNING, covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from testTable where state = \"CA\"";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 200/*numReadKeys*/);

        expCnt = 1;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE, false, null);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 2. ALL_SHARDS delete, with RETURNING, covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from testTable where state = \"CA\" returning id";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 100;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE, false, null);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 3 ALL_SHARDS delete, with RETURNING, non-covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from testTable where state = \"CA\" " +
                "returning sid, id, name";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 100 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 100;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE, false, null);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 4. ALL_SHARDS delete, without RETURNING, non-covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from testTable where state = \"CA\" and name != \"abc\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 100 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 1;
        maxReadKBs = new int[] {13};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE, false, null);
        }
    }

    @Test
    public void testInsert() {
        final int numMajor = 1;
        final int numPerMajor = 10;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        QueryRequest req;
        QueryResult ret;

        /* Insert a new row */
        int newRecordKB = 8;
        String longString = genString(newRecordKB * 1024);
        String query =
            "insert into testTable values " +
            "(1, 15, \"myname\", 23, \"WI\", 2500, [], \"" +
            longString + "\")";

        req = newQueryRequest();
        req.setStatement(query);
        ret = asyncHandle.query(req).join();

        assertEquals(1, ret.getResults().size());

        query = "select sid, id, name from testTable where id = 15";
        req = newQueryRequest();
        req.setStatement(query);
        ret = asyncHandle.query(req).join();
        assertEquals(1, ret.getResults().size());
        MapValue res = ret.getResults().get(0);
        FieldValue name = res.get("name");
        assertEquals("myname", name.getString());
    }

    @Test
    public void testUpdatePrepared() throws Throwable {
        assumeKVVersion("testUpdatePrepared", 21, 3, 1);
        final int numMajor = 1;
        final int numPerMajor = 10;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);
        String longString = genString(1024);
        /* Update using preparedStatement */
        String query = "declare $sval string; $sid integer; $id integer;" +
            "update testTable set longString = $sval " +
            "where sid = $sid and id = $id returning sid";
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(query);

        final AtomicReference<QueryRequest> req = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        asyncHandle.prepare(prepReq)
            .thenCompose(prepRet -> {
                assertNotNull(prepRet.getPreparedStatement());
                prepRet.getPreparedStatement()
                    .setVariable("$sval", new StringValue(longString))
                    .setVariable("$sid", new IntegerValue(0))
                    .setVariable("$id", new IntegerValue(1));
                req.set(newQueryRequest());
                req.get().setPreparedStatement(prepRet);
                return asyncHandle.query(req.get());
            })
            .thenApply(res -> {
                assertNotNull(res.getResults());
                return res;
            })
            .whenComplete((res, err) -> {
                if (err != null) {
                    ex.set(err);
                }
                req.get().close();
                latch.countDown();
            });
        latch.await();
        if (ex.get() != null) {
            throw ex.get();
        }
    }

    @Test
    public void testPreparedLongRunning() {
        final int numMajor = 1;
        final int numPerMajor = 10;
        final int recordKB = 2;

        /* This test is only run in specific configurations */
        assumeTrue(Boolean.getBoolean("test.longrunning"));

        /* Load rows to table */
        verbose("Loading rows into table...");
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);
        verbose("Loaded all rows");
        String longString = genString(1024);

        /* Update using preparedStatement */
        String query = "declare $sval string; $sid integer; $id integer;" +
            "update testTable set longString = $sval " +
            "where sid = $sid and id = $id returning sid";
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(query);
        PrepareResult prepRet = asyncHandle.prepare(prepReq).join();

        PreparedStatement ps = prepRet.getPreparedStatement();
        assertNotNull(ps);

        int total=0;
        int passed=0;
        int exceptions=0;
        int timeouts=0;
        int nullResults=0;
        boolean lastPassed = false;

        long runMs = Long.getLong("test.runms", 100000);
        long delayMs = Long.getLong("test.delayms", 100);

        /* run for N milliseconds, with M milliseconds delay between queries */
        long startMs = System.currentTimeMillis();
        while (true) {
            lastPassed = false;
            ps.setVariable("$sval", new StringValue(longString))
                .setVariable("$sid", new IntegerValue(0))
                .setVariable("$id", new IntegerValue(1));

            try (QueryRequest req = newQueryRequest()) {
                req.setPreparedStatement(prepRet);
                total++;
                verbose("Running query #" + total + "...");
                QueryResult res = ConcurrentUtil.awaitFuture(
                    asyncHandle.query(req));
                if (res == null) {
                    verbose(" got null result");
                    nullResults++;
                } else {
                    passed++;
                    lastPassed = true;
                }
            } catch (RequestTimeoutException rte) {
                /* timeouts are (possibly) expected */
                timeouts++;
                verbose(" got request timeout");
            } catch (Exception e) {
                exceptions++;
                verbose(" got exception: " + e);
            }
            if ((System.currentTimeMillis() - startMs) > runMs) {
                break;
            }
            try {
                verbose("Sleeping for " + delayMs + "ms...");
                Thread.sleep(delayMs);
            } catch (Exception unused) {}
        }
        verbose("Finished: total=" + total + ", pass=" + passed +
                ", timeouts=" + timeouts + ", exceptions=" + exceptions +
                ", nullResults=" + nullResults);
        assertEquals("Unexpected number of exceptions. Expected zero, got " +
                exceptions, 0, exceptions);
        assertEquals("Unexpected number of null results. Expected zero, got " +
                nullResults, 0, nullResults);
        assertTrue("Expected last request to pass, but it failed", lastPassed);
    }

    /**
     * Returns the estimated readKB.
     */
    private int getExpReadKB(boolean keyOnly,
                             int recordKB,
                             int numReadRows,
                             int numReadKeys) {
        final int minRead = 1;
        int readKB = numReadKeys * minRead;
        if (!keyOnly) {
            readKB += numReadRows * recordKB;
        }
        return readKB == 0 ? minRead : readKB;
    }

    /*
     * Test illegal cases -- both prepared statement and string
     */
    @Test
    public void testIllegalQuery() {

        PrepareRequest prepReq;
        QueryRequest queryReq;
        String query;

        final String queryWithVariables =
            "declare $sid integer; $id integer;" +
            "select name from testTable where sid = $sid and id >= $id";

        /* Syntax error */
        prepReq = new PrepareRequest().setStatement("random string");
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.prepare(prepReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {}

        queryReq = newQueryRequest();
        queryReq.setStatement("random string");
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {}

        /* Try a query that requires external variables that are missing */
        queryReq = newQueryRequest();
        queryReq.setStatement(queryWithVariables);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        prepReq = new PrepareRequest().setStatement(queryWithVariables);
        PrepareResult prepRes = handle.prepare(prepReq);
        queryReq = newQueryRequest();
        queryReq.setPreparedStatement(prepRes);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae)  {
        }

        /* Wrong name of variables */
        prepReq = new PrepareRequest().setStatement(queryWithVariables);
        prepRes = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRes.getPreparedStatement();
        prepStmt.setVariable("sid", new IntegerValue(9));
        prepStmt.setVariable("id", new IntegerValue(3));
        queryReq = newQueryRequest();
        queryReq.setPreparedStatement(prepRes);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException ex) {
        }

        /* Wrong type for variables */
        prepReq = new PrepareRequest().setStatement(queryWithVariables);
        prepRes = handle.prepare(prepReq);
        prepStmt = prepRes.getPreparedStatement();
        prepStmt.setVariable("$sid", new DoubleValue(9.1d));
        prepStmt.setVariable("$id", new IntegerValue(3));
        queryReq = newQueryRequest();
        queryReq.setPreparedStatement(prepRes);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /* Table not found */
        query = "select * from invalidTable";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.prepare(prepReq));
            fail("prepare should have failed");
        } catch (TableNotFoundException tnfe) {
        }

        queryReq = newQueryRequest();
        queryReq.setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (TableNotFoundException tnfe) {
        }

        /* Invalid column */
        query = "select * from testTable where invalidColumn = 1";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.prepare(prepReq));
            fail("prepare should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = newQueryRequest();
        queryReq.setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException tnfe) {
        }

        /* Prepare or execute Ddl statement */
        query = "create table t1(id integer, name string, primary key(id))";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.prepare(prepReq));
            fail("prepare should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = newQueryRequest();
        queryReq.setStatement(query);
        try {
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = newQueryRequest();
        queryReq.setStatement(query);
        try {
            queryReq.setLimit(-1);
            ConcurrentUtil.awaitFuture(asyncHandle.query(queryReq));
            fail("QueryRequest.setLimit() should fail with IAE");
        } catch (IllegalArgumentException iae) {
        }
        queryReq.setLimit(0);

        try {
            queryReq.setMaxReadKB(-1);
            fail("QueryRequest.setMaxReadKB() should fail with IAE");
        } catch (IllegalArgumentException iae) {
        }


        /*
         * Namespaces, child tables and identity columns are not
         * yet supported
         */
        String statement =
            "create table ns:foo(id integer, primary key(id))";
        try {
            tableOperationAsync(asyncHandle, statement,
                           new TableLimits(10, 10, 10)).join();
            fail("Namespaces not supported in table names");
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("namespace"));
        }

        statement = "drop table ns:foo";
        try {
            tableOperationAsync(asyncHandle, statement,
                           new TableLimits(10, 10, 10)).join();
            fail("Namespaces not supported in table names");
        } catch (Exception e) {
            if (onprem) {
                assertTrue(e.getCause() instanceof TableNotFoundException);
            } else {
                assertTrue(e.getMessage().toLowerCase()
                           .contains("namespace"));
            }
        }

        statement = "select * from ns:foo";
        try {
            executeQuery(statement, null, 0, 0, false);
            fail("Query with namespaced table not supported");
        } catch (Throwable t) {
            if (onprem) {
                assertTrue(t instanceof TableNotFoundException);
            } else {
                assertTrue(t.getMessage().toLowerCase()
                           .contains("namespace"));
            }
        }

        statement = "create namespace myns";
        try {
            tableOperationAsync(asyncHandle, statement,
                           new TableLimits(10, 10, 10)).join();
            if (!onprem) {
                fail("Creating namespaces not supported");
            }
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("namespace"));
        }

        statement = "drop namespace myns";
        try {
            tableOperationAsync(asyncHandle, statement,
                           new TableLimits(10, 10, 10)).join();
            if (!onprem) {
                fail("Dropping namespaces not supported");
            }
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("namespace"));
        }

        statement = "create table a.foo(id integer, primary key(id))";
        try {
            tableOperationAsync(asyncHandle, statement,
                           new TableLimits(10, 10, 10)).join();
            fail("Child tables not supported in table names");
        } catch (Exception e) {
            assertTrue((e.getCause() instanceof TableNotFoundException) ||
                       (e.getCause() instanceof IllegalArgumentException));
        }
    }

    @Test
    public void testJson() {
        final String[] jsonRecords = {
            "{" +
            " \"id\":0," +
            " \"info\":" +
            "  {" +
            "    \"firstName\":\"first0\", \"lastName\":\"last0\",\"age\":10," +
            "    \"address\":" +
            "    {" +
            "      \"city\": \"San Fransisco\"," +
            "      \"state\"  : \"CA\"," +
            "      \"phones\" : [" +
            "                     { \"areacode\" : 408, \"number\" : 50," +
            "                       \"kind\" : \"home\" }," +
            "                     { \"areacode\" : 650, \"number\" : 51," +
            "                       \"kind\" : \"work\" }," +
            "                     \"650-234-4556\"," +
            "                     650234455" +
            "                   ]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"Anna\" : { \"age\" : 10, \"school\" : \"sch_1\"," +
            "               \"friends\" : [\"Anna\", \"John\", \"Maria\"]}," +
            "      \"Lisa\" : { \"age\" : 12, \"friends\" : [\"Ada\"]}" +
            "    }" +
            "  }" +
            "}",

            "{" +
            "  \"id\":1," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first1\", \"lastName\":\"last1\",\"age\":11," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Boston\"," +
            "      \"state\"  : \"MA\"," +
            "      \"phones\" : [ { \"areacode\" : 304, \"number\" : 30," +
            "                       \"kind\" : \"work\" }," +
            "                     { \"areacode\" : 318, \"number\" : 31," +
            "                       \"kind\" : \"work\" }," +
            "                     { \"areacode\" : 400, \"number\" : 41," +
            "                       \"kind\" : \"home\" }]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"Anna\" : { \"age\" : 9,  \"school\" : \"sch_1\"," +
            "                   \"friends\" : [\"Bobby\", \"John\", null]}," +
            "      \"Mark\" : { \"age\" : 4,  \"school\" : \"sch_1\"," +
            "                   \"friends\" : [\"George\"]}," +
            "      \"Dave\" : { \"age\" : 15, \"school\" : \"sch_3\"," +
            "                   \"friends\" : [\"Bill\", \"Sam\"]}" +
            "    }" +
            "  }" +
            "}",

            "{" +
            "  \"id\":2," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first2\", \"lastName\":\"last2\",\"age\":12," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Portland\"," +
            "      \"state\"  : \"OR\"," +
            "      \"phones\" : [ { \"areacode\" : 104, \"number\" : 10," +
            "                       \"kind\" : \"home\" }," +
            "                     { \"areacode\" : 118, \"number\" : 11," +
            "                       \"kind\" : \"work\" } ]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "    }" +
            "  }" +
            "}",

            "{ " +
            "  \"id\":3," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first3\", \"lastName\":\"last3\",\"age\":13," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Seattle\"," +
            "      \"state\"  : \"WA\"," +
            "      \"phones\" : null" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"George\" : { \"age\" : 7,  \"school\" : \"sch_2\"," +
            "                     \"friends\" : [\"Bill\", \"Mark\"]}," +
            "      \"Matt\" :   { \"age\" : 14, \"school\" : \"sch_2\"," +
            "                     \"friends\" : [\"Bill\"]}" +
            "    }" +
            "  }" +
            "}"
        };

        String query;
        Map<String, FieldValue> bindValues = new HashMap<String, FieldValue>();

        tableOperationAsync(asyncHandle, createJsonTableDDL,
                       new TableLimits(15000, 15000, 50)).join();

        loadRowsToTable(jsonTable, jsonRecords);

        /* Basic query on a table with JSON field */
        query = "select id, f.info from jsonTable f";
        executeQuery(query, null, 4, 0, false /* usePrepStmt */);

        /* Test JsonNull */
        query = "select id from jsonTable f where f.info.address.phones = null";
        executeQuery(query, null, 1, 0, false /* usePrepStmt */);

        /* Bind JsonNull value */
        query = "declare $phones json;" +
            "select id, f.info.address.phones " +
            "from jsonTable f " +
            "where f.info.address.phones != $phones";
        bindValues.put("$phones", JsonNullValue.getInstance());
        executeQuery(query, bindValues, 3, 0, true /* usePrepStmt */);

        /* Bind 2 String values */
        query = "declare $city string;$name string;" +
            "select id, f.info.address.city, f.info.children.keys() " +
            "from jsonTable f " +
            "where f.info.address.city = $city and " +
            "      not f.info.children.keys() =any $name";
        bindValues.clear();
        bindValues.put("$city", new StringValue("Portland"));
        bindValues.put("$name", new StringValue("John"));
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);

        /* Bind MapValue */
        query = "declare $child json;" +
                "select id, f.info.children.values() " +
                "from jsonTable f " +
                "where f.info.children.values() =any $child";
        String json = "{\"age\":14, \"school\":\"sch_2\", " +
                      " \"friends\":[\"Bill\"]}";
        bindValues.clear();
        bindValues.put("$child", JsonUtils.createValueFromJson(json, null));
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);

        /* Bind ArrayValue */
        query = "declare $friends json;" +
            "select id, f.info.children.values() " +
            "from jsonTable f " +
            "where f.info.children.values().friends =any $friends";

        ArrayValue friends = new ArrayValue();
        friends.add("Bill");
        friends.add("Mark");
        bindValues.clear();
        bindValues.put("$friends", friends);
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);
    }

    @Test
    public void testPrepare() {
        String query;
        PrepareRequest req;
        PrepareResult ret;

        query = "select * from testTable";
        req = new PrepareRequest().setStatement(query);
        ret = asyncHandle.prepare(req).join();
        if (!onprem) {
            assertEquals(ret.getReadKB(), getMinQueryCost());
            assertEquals(0, ret.getWriteKB());
        }

        query = "declare $sval string; $sid integer; $id integer;" +
                "update testTable set longString = $sval " +
                "where sid = $sid and id = $id";
        req = new PrepareRequest().setStatement(query);
        ret = asyncHandle.prepare(req).join();
        if (!onprem) {
            assertEquals(ret.getReadKB(), getMinQueryCost());
            assertEquals(0, ret.getWriteKB());
        }
    }

    /**
     * Prepare a query, use it, evolve table, try again.
     */
    @Test
    public void testEvolution() {

        /* Load rows to table */
        loadRowsToScanTable(1, 10, 2);
        String query = "select age from testTable";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = asyncHandle.prepare(prepReq).join();
        assertNotNull(prepRet.getPreparedStatement());

        try(QueryRequest qreq = newQueryRequest()) {
            qreq.setPreparedStatement(prepRet);
            QueryResult qres = asyncHandle.query(qreq).join();
            assertEquals(10, qres.getResults().size());

            /*
             * Evolve and try the query again. It will fail because table schema
             * has been changed, the query need to be prepared again.
             *
             * The exception caught from query may vary depending on the
             * different proxy and KV version. PrepareQueryException will be
             * thrown for proxy(KVClient 25.1.1) + KV server 25.1.1 or higher.
             * Otherwise, IllegalArgumentException will be thrown.
             */
            tableOperationAsync(asyncHandle,
                "alter table testTable(drop age)", null).join();

            try {
                ConcurrentUtil.awaitFuture(asyncHandle.query(qreq));
                fail("Query should have failed");
            } catch (PrepareQueryException | IllegalArgumentException ex) {
                /*
                 * If can't determine the versions of KV client and server, skip
                 * checking the specific exception.
                 */
                if (getMinimumKVVersion() > 0) {
                    if (checkKVVersion(25, 1, 1)) {
                        assertTrue(ex instanceof PrepareQueryException);
                    } else {
                        assertTrue(ex instanceof IllegalArgumentException);
                    }
                }
            }
        }
    }

    @Test
    public void testIdentityAndUUID() {
        String idName = "testSG";
        String uuidName = "testUUID";
        String createTableId =
            "CREATE TABLE " + idName +
                "(id INTEGER GENERATED ALWAYS AS IDENTITY, " +
                 "name STRING, " +
                 "PRIMARY KEY(id))";
        String createTableUUID =
            "CREATE TABLE " + uuidName +
                "(id STRING AS UUID GENERATED BY DEFAULT, " +
                 "name STRING, " +
                 "PRIMARY KEY(id))";

        tableOperationAsync(asyncHandle, createTableId,
            new TableLimits(100, 100, 1)).join();

        /*
         * Putting a row with a value for "id" should fail because always
         * generated identity column should not has value.
         */
        MapValue value = new MapValue().put("id", 100).put("name", "abc");
        PutRequest putReq = new PutRequest().setTableName(idName);
        try {
            putReq.setValue(value);
            ConcurrentUtil.awaitFuture(asyncHandle.put(putReq));
            fail("Expected IAE; a generated always identity " +
                 "column should not have a value");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Putting a row without "id" field should succeed.
         */
        value = new MapValue().put("name", "abc");
        putReq.setValue(value);
        PutResult putRet = asyncHandle.put(putReq).join();
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());

        if (!checkKVVersion(20, 3, 1)) {
            return;
        }

        tableOperationAsync(asyncHandle, createTableUUID,
                            new TableLimits(100, 100, 1)).join();

        /*
         * Now the UUID table
         */
        value = new MapValue().put("id", "abcde").put("name", "abc");
        putReq = new PutRequest().setTableName(uuidName);
        try {
            putReq.setValue(value);
            ConcurrentUtil.awaitFuture(asyncHandle.put(putReq));
            fail("Expected IAE; the uuid value set was not a uuid");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Putting a row without "id" field should succeed.
         */
        value = new MapValue().put("name", "abc");
        putReq.setValue(value);
        putRet = asyncHandle.put(putReq).join();
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());
    }

    @Test
    public void testQueryOrder() {

        final String[] declOrder = {
            "sid", "id", "name", "age", "state","salary", "array", "longString"
        };

        /* Load rows to table */
        loadRowsToScanTable(10, 10, 1);

        try (QueryRequest queryReq = newQueryRequest()) {
            queryReq.setStatement(
                "select * from testTable where id = 1 and sid = 1");

            QueryResult queryRes = asyncHandle.query(queryReq).join();

            /*
             * For each result, assert that the fields are all there and in the
             * expected order.
             */
            for (MapValue v : queryRes.getResults()) {
                assertEquals(declOrder.length, v.size());
                int i = 0;
                for (Map.Entry<String, FieldValue> entry : v.entrySet()) {
                    assertEquals(declOrder[i++], entry.getKey());
                }

                /* perform a get and validate that it also is in decl order */
                GetRequest getReq = new GetRequest()
                    .setTableName(tableName)
                    .setKey(v);
                GetResult getRes = asyncHandle.get(getReq).join();
                i = 0;
                for (Map.Entry<String, FieldValue> entry :
                         getRes.getValue().entrySet()) {
                    assertEquals(declOrder[i++], entry.getKey());
                }
            }
        }
    }

    @Test
    public void testLowThroughput() {
        if (onprem == false) {
            assumeKVVersion("testLowThroughput", 21, 3, 1);
        }
        final int numRows = 30;
        String name = "testThroughput";
        String createTableDdl =
            "CREATE TABLE " + name +
            "(id INTEGER, bin binary, json json, primary key(id))";

        tableOperationAsync(asyncHandle, createTableDdl,
                            new TableLimits(2, 20000, 1)).join();

        MapValue value = new MapValue()
            .put("bin", new byte[3000])
            .put("json", "abc");
        PutRequest putReq = new PutRequest().setTableName(name);

        /* add rows */
        for (int i = 0; i < numRows; i++) {
            value.put("id", i);
            putReq.setValue(value);
            PutResult putRet = asyncHandle.put(putReq).join();
            assertNotNull(putRet.getVersion());
        }

        /*
         * Ensure that this query completes.
         * 30 rows of 3K+ each = ~90KB.
         * at 2RUs/sec, that's about 45 seconds.
         */
        final String stmt = "select * from " + name;
        executeQuery(stmt, null, numRows, 0, false);
    }

    /*
     * Tests that a query with a V2 sort (geo_near) can operate against
     * query versions 2 and 3
     */
    @Test
    public void testQueryCompat() {
        final String geoTable = "create table points (id integer, " +
            "info json, primary key(id))";
        final String geoIndex =
            "create index idx_ptn on points(info.point as point)";
        final String geoQuery =
            "select id from points p " +
            "where geo_near(p.info.point, " +
            "{ \"type\" : \"point\", \"coordinates\" : [24.0175, 35.5156 ]}," +
            "5000)";

        TableResult tres = tableOperationAsync(asyncHandle, geoTable,
                                          new TableLimits(4, 1, 1)).join();
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        tres = tableOperationAsync(asyncHandle, geoIndex, null).join();
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PrepareRequest prepReq = new PrepareRequest().setStatement(geoQuery);
        PrepareResult prepRet = asyncHandle.prepare(prepReq).join();
        assertNotNull(prepRet.getPreparedStatement());
    }

    /*
     * Test use of large query strings for insert/update/upsert
     */
    @Test
    public void testLargeQueryStrings() {
        if (onprem) {
            assumeKVVersion("testLargeQueryStrings", 20, 1, 1);
        } else {
            assumeKVVersion("testLargeQueryStrings", 21, 3, 1);
        }
        final String tableName = "LargeQuery";
        final String createTable = "create table " + tableName +
            "(id integer, data json, primary key(id))";
        final int[] stringSizes = {10, 500, 5000, 20000, 500000};

        tableOperationAsync(asyncHandle, createTable,
            new TableLimits(4, 1000, 1000)).join();
        /* create a large JSON data string */
        for (int size : stringSizes) {
            String data = createLargeJson(size);
            String iquery = "insert into " + tableName + " values(1," +
                data + ") returning id";
            String uquery = "update " + tableName + " t " +
                "set t.data = " + data + "where id = 1 returning id";

            /* insert, then update */
            QueryRequest req = newQueryRequest();
            req.setStatement(iquery);
            QueryResult res = asyncHandle.query(req).join();
            assertEquals(1, res.getResults().get(0).get("id").getInt());
            req = newQueryRequest();
            req.setStatement(uquery);
            res = asyncHandle.query(req).join();
            assertEquals(1, res.getResults().get(0).get("id").getInt());
        }

        /* validate that select fails */
        final String squery = "select * from " + tableName +
            " t where t.data.data = " + genString(15000);

        try (QueryRequest req = newQueryRequest()) {
            req.setStatement(squery);
            ConcurrentUtil.awaitFuture(asyncHandle.query(req));
            fail("Query should have failed");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
    }

    @Test
    public void testBindArrayValue() {
        if (!arrayAsRecordSupported) {
            return;
        }
        assumeKVVersion("testBindArrayValue", 20, 3, 1);
        final String tableName = "testBindArrayValue";
        final String createTable = "create table if not exists " + tableName +
                "(id integer, " +
                 "info record(name string, age integer, " +
                             "address record(street string, room integer)), " +
                 "primary key(id))";

        tableOperationAsync(asyncHandle, createTable,
            new TableLimits(100, 100, 1)).join();

        String stmt = "declare $id integer;" +
                      "$info record(name string, age integer, " +
                                   "address record(street string, " +
                                                  "room integer));" +
                      "upsert into " + tableName + " values($id, $info)";
        PrepareRequest prepReq = new PrepareRequest().setStatement(stmt);
        PrepareResult prepRet = asyncHandle.prepare(prepReq).join();
        PreparedStatement pstmt = prepRet.getPreparedStatement();

        MapValue mapVal;
        int id = 0;

        /* Case1: all fields are specified with non-null value */
        ArrayValue adVal = new ArrayValue()
                .add("35 Network drive")
                .add(203);
        ArrayValue arrVal = new ArrayValue()
                .add("Jack Wang")
                .add(40)
                .add(adVal);
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", arrVal.get(1))
                .put("address",
                     new MapValue().put("street", adVal.get(0))
                                   .put("room", adVal.get(1)));
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /* Case2: address = NULL*/
        arrVal = new ArrayValue()
                .add("Jack Wang")
                .add(40)
                .add(NullValue.getInstance());
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", arrVal.get(1))
                .put("address", NullValue.getInstance());
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /*
         * Case3: age = "40" and address.room = "203" which are castable to
         *        integer
         */
        adVal = new ArrayValue()
                .add("35 Network drive")
                .add("203");
        arrVal = new ArrayValue()
                .add("Jack Wang")
                .add("40")
                .add(adVal);
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", 40)
                .put("address",
                     new MapValue().put("street", adVal.get(0))
                                   .put("room", 203));
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /*
         * Negative cases
         */
        /* info.name: Type mismatch on input. Expected STRING, got INTEGER */
        arrVal = new ArrayValue()
                .add(40)
                .add("Jack Wang")
                .add(NullValue.getInstance());
        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", arrVal);

        try (QueryRequest req = newQueryRequest()) {
            req.setPreparedStatement(pstmt);
            handle.query(req);
            fail("Expected IAE");
        } catch (IllegalArgumentException ex) {
        }

        /*
         * Invalid Array value for Record Value, it has 1 element but
         * the Record Value contains 3 fields
         */
        arrVal = new ArrayValue()
                .add("Jack Wang");
        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", arrVal);

        try (QueryRequest req = newQueryRequest()) {
            req.setPreparedStatement(pstmt);
            ConcurrentUtil.awaitFuture(asyncHandle.query(req));
            fail("Expected IAE");
        } catch(IllegalArgumentException ex) {
        }
    }

    @Test
    public void testUsabilityQueryPaginator() throws Exception {
        /* Load rows to table */
        loadRowsToScanTable(3, 2, 1);

        QueryRequest qreq = newQueryRequest();
        qreq.setStatement("select * from testTable").setLimit(3);

        final QueryPaginatorResult pageResult = asyncHandle.queryPaginator(qreq);
        Flow.Publisher<List<MapValue>> publisher = pageResult.getResults();
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        List<MapValue> items = subscriber.awaitItems(1, TimeUnit.SECONDS);
        assertEquals(6, items.size());
        items.forEach(Assert::assertNotNull);
        assertTrue(qreq.isDone());
    }

    @Test
    public void testQueryPaginator() throws Exception {
        final int numMajor = 10;
        final int numPerMajor = 1;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, 1);

        /* check simple queries */
        checkQueryPaginatorUnordered("select * from testTable");

        /* check non-simple queries, ie. group by, order by */
        checkQueryPaginatorUnordered("select sid, count(*) from testTable " +
            "group by sid");

        checkQueryPaginatorOrdered("select * from testTable order by sid, " +
            "id");
        checkQueryPaginatorOrdered("select sid, count(*) from testTable group" +
            " by sid order by sid");
    }

    /**
     * Runs query twice and checks if results from queryIterable iterator are
     * the same as regular query() results.
     */
    private void checkQueryPaginatorUnordered(String query) throws Exception {

        QueryResultDTO qiDTO = new QueryResultDTO();

        try (QueryRequest qreq = newQueryRequest()) {
            qreq.setStatement(query).setLimit(3);
            processQuery(qreq, 0 /* limit */, qiDTO).join();
        }

        QueryRequest qreq = newQueryRequest();
        qreq.setStatement(query).setLimit(3);

        QueryPaginatorResult qipage = asyncHandle.queryPaginator(qreq);
        TestSubscriber subscriber = new TestSubscriber();
        qipage.getResults().subscribe(subscriber);
        subscriber.awaitItems(5, TimeUnit.SECONDS);

        Set<MapValue> expectedSet = Set.copyOf(qiDTO.items);
        Set<MapValue> actualSet = Set.copyOf(subscriber.items);

        try {
            assertEquals(expectedSet.size(), actualSet.size());
        } catch (Throwable ex) {
            for (MapValue r : actualSet) {
                if (!expectedSet.contains(r)) {
                    System.out.println("Fail: actual row not " +
                        "expected: " + r);
                }
            }
            for (MapValue r : expectedSet) {
                if (!actualSet.contains(r)) {
                    System.out.println("Fail: expected row not found: " + r);
                }
            }
            fail("rows not matching: expected size: " +
                expectedSet.size() + "   actual size: " + actualSet.size());
        }

        assertEquals(expectedSet, actualSet);

        assertEquals(qiDTO.totalRateLimitDelay.intValue(),
            qipage.getRateLimitDelayedMs());
        assertEquals(qiDTO.readKB.intValue(), qipage.getReadKB());
        assertEquals(qiDTO.writeKB.intValue(), qipage.getWriteKB());
        assertEquals(qiDTO.readUnits.intValue(), qipage.getReadUnits());
        assertEquals(qiDTO.writeUnits.intValue(), qipage.getWriteUnits());
        assertEquals(qiDTO.totalRetryStats, qipage.getRetryStats());
    }

    /**
     * Runs query twice and checks if results from queryIterable iterator are
     * the same as regular query() results.
     */
    private void checkQueryPaginatorOrdered(String query) throws Exception {
        QueryRequest qipageReq = newQueryRequest();
        qipageReq.setStatement(query);
        QueryResultDTO dto = new QueryResultDTO();

        try (QueryRequest qreq = newQueryRequest()) {
            qreq.setStatement(query);
            processQuery(qreq, 0 /* limit */, dto).join();
        }

        QueryPaginatorResult qipage = asyncHandle.queryPaginator(qipageReq);
        TestSubscriber subscriber = new TestSubscriber();
        qipage.getResults().subscribe(subscriber);
        subscriber.awaitItems(5, TimeUnit.SECONDS);

        List<MapValue> expectedRows = dto.items;
        List<MapValue> actualRows = subscriber.items;

        try {
            assertEquals(expectedRows.size(), actualRows.size());
        } catch (Throwable ex) {
            for (MapValue r : actualRows) {
                if (!expectedRows.contains(r)) {
                    System.out.println("Fail: actual row not " +
                            "expected: " + r);
                }
            }
            for (MapValue r : expectedRows) {
                if (!actualRows.contains(r)) {
                    System.out.println("Fail: expected row not found: " + r);
                }
            }
            fail("rows not matching: expected size: " +
                    expectedRows.size() + "   actual size: " + actualRows.size());
        }
        assertEquals(dto.readKB.intValue(), qipage.getReadKB());
        assertEquals(dto.writeKB.intValue(), qipage.getWriteKB());
        assertEquals(dto.readUnits.intValue(), qipage.getReadUnits());
        assertEquals(dto.writeUnits.intValue(), qipage.getWriteUnits());
        assertEquals(dto.totalRetryStats, qipage.getRetryStats());
    }

    private void execInsertAndCheckInfo(PreparedStatement pstmt,
                                        int id,
                                        FieldValue info,
                                        String tableName,
                                        MapValue expInfo) {

        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", info);

        try (QueryRequest req = newQueryRequest()) {
            req.setPreparedStatement(pstmt);
            QueryResult ret = handle.query(req);
            assertEquals(1, ret.getResults().get(0).asMap()
                .get("NumRowsInserted").getInt());
        }

        String stmt = "select info from " + tableName + " where id = " + id;
        try (QueryRequest req = newQueryRequest()) {
            req.setStatement(stmt);
            QueryResult ret = handle.query(req);
            assertEquals(1, ret.getResults().size());
            assertEquals(expInfo, ret.getResults().get(0).get("info"));
        }
    }

    private String createLargeJson(int size) {
        MapValue map = new MapValue();
        map.put("data", genString(size));
        return map.toString();
    }

    private void executeQuery(String statement,
                              boolean keyOnly,
                              boolean indexScan,
                              int expNumRows,
                              int expReadKB,
                              int numLimit,
                              int sizeLimit,
                              int recordKB) {
        executeQuery(statement, keyOnly, indexScan, expNumRows, expReadKB,
                     numLimit, sizeLimit, recordKB, Consistency.EVENTUAL,
                     false, null);
        executeQuery(statement, keyOnly, indexScan, expNumRows, expReadKB,
                     numLimit, sizeLimit, recordKB, Consistency.ABSOLUTE,
                     false, null);
    }

    private void executeQuery(String query,
            Map<String, FieldValue> bindValues,
            int expNumRows,
            int maxReadKB,
            boolean usePrepStmt) {
        executeQuery(query, false, false, expNumRows, 0, /* expReadKB */
                     0 /* numLimits */, maxReadKB, 0 /* recordKB */,
                     null /* consistency */, usePrepStmt, bindValues);
    }
    private void executeQuery(String statement,
                              boolean keyOnly,
                              boolean indexScan,
                              int expNumRows,
                              int expReadKB,
                              int numLimit,
                              int sizeLimit,
                              int recordKB,
                              Consistency consistency,
                              boolean usePrepStmt,
                              Map<String, FieldValue> bindValues) {

        try (final QueryRequest queryReq = newQueryRequest()) {
            if (bindValues == null || !usePrepStmt) {
                queryReq
                    .setStatement(statement).setLimit(numLimit)
                    .setConsistency(consistency)
                    .setMaxReadKB(sizeLimit);
            } else {
                PrepareRequest prepReq =
                    new PrepareRequest().setStatement(statement);
                PrepareResult prepRes = asyncHandle.prepare(prepReq).join();
                PreparedStatement prepStmt = prepRes.getPreparedStatement();
                if (bindValues != null) {
                    for (Entry<String, FieldValue> entry : bindValues
                        .entrySet()) {
                        prepStmt.setVariable(entry.getKey(), entry.getValue());
                    }
                }
                queryReq
                    .setPreparedStatement(prepStmt)
                    .setMaxReadKB(sizeLimit)
                    .setLimit(numLimit)
                    .setConsistency(consistency);
            }

            final QueryResultDTO dto = new QueryResultDTO();
            final AtomicReference<Throwable> ex = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            processQuery(queryReq, numLimit, dto)
                .thenAccept( ignored -> {
                    if (showResults) {
                        System.out.println(
                            "Total ReadKB = " +dto.readKB.intValue() +
                            " Total ReadUnits = " + dto.readUnits.intValue() +
                            " Total WriteKB = " + dto.writeKB.intValue());
                    }

                    assertEquals("Wrong number of rows returned, expect "
                        + expNumRows + ", but get " + dto.numRows.intValue(),
                        dto.numRows.intValue(), expNumRows);
                })
                .whenComplete((v, err) -> {
                    if (err != null) {
                        ex.set(err.getCause());
                    }
                    latch.countDown();
                });
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (ex.get() != null) {
                throw new RuntimeException(ex.get());
            }
        }
    }

    private void loadRowsToScanTable(int numMajor, int numPerMajor, int nKB) {

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);

        String[] states = { "CA", "OR", "WA", "VT", "NY" };
        int[] salaries = { 1000, 15000, 8000, 9000 };
        ArrayValue[] arrays = new ArrayValue[4];

        for (int i = 0; i < 4; ++i) {
            arrays[i] = new ArrayValue(4);
        }
        arrays[0].add(1).add(5).add(7).add(10);
        arrays[1].add(4).add(7).add(7).add(11);
        arrays[2].add(3).add(8).add(17).add(21);
        arrays[3].add(3).add(8).add(12).add(14);

        int slen = (nKB - 1) * 1024;
        /* Load rows */
        for (int i = 0; i < numMajor; i++) {
            value.put("sid", i);
            for (int j = 0; j < numPerMajor; j++) {
                value.put("id", j);
                value.put("name", "name_" + j);
                value.put("age", j % 10);
                value.put("state", states[j % 5]);
                value.put("salary", salaries[j % 4]);
                value.put("array", arrays[j % 4]);
                value.put("longString", genString(slen));
                PutResult res = asyncHandle.put(putRequest).join();
                assertNotNull("Put failed", res.getVersion());
            }
        }
    }

    private void loadRowsToTable(String tabName, String[] jsons) {

        for (String json : jsons) {
            MapValue value = (MapValue)JsonUtils.createValueFromJson(json, null);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tabName);
            PutResult res = asyncHandle.put(putRequest).join();
            assertNotNull("Put failed", res.getVersion());
        }
    }

    private static int getMinQueryCost() {
        return MIN_QUERY_COST;
    }

    @SuppressWarnings("resource")
	private QueryRequest newQueryRequest() {
        return new QueryRequest().setTraceLevel(traceLevel);
    }

    CompletableFuture<Void> processQuery(QueryRequest queryRequest,
                                         int numLimit,
                                         QueryResultDTO dto) {
        return asyncHandle.query(queryRequest)
            .thenComposeAsync(queryRes -> {
                final List<MapValue> results = queryRes.getResults();
                final int cnt = results.size();
                if (numLimit > 0) {
                    assertTrue(
                        "Unexpected number of rows returned, expect <= " +
                            numLimit + ", but get " + cnt + " rows",
                        cnt <= numLimit);
                }
                final int rkb = queryRes.getReadKB();
                final int runits = queryRes.getReadUnits();
                final int wkb = queryRes.getWriteKB();
                final int wunits = queryRes.getWriteUnits();

                if (showResults) {
                    for (int i = 0; i < results.size(); ++i) {
                        System.out.println("Result "
                            + (dto.numRows.intValue() + i) + " :");
                        System.out.println(results.get(i));
                    }
                    System.out.println("Batch " + dto.numBatches.intValue() +
                        " ReadKB=" + rkb +
                        " ReadUnits=" + runits +
                        " WriteKB=" + wkb +
                        " WriteUnits=" + wunits);
                }
                dto.items.addAll(results);
                dto.numRows.add(cnt);
                dto.readKB.add(rkb);
                dto.readUnits.add(runits);
                dto.writeKB.add(wkb);
                dto.writeUnits.add(wunits);
                dto.numBatches.add(1);
                dto.totalRateLimitDelay.add(queryRes.getRateLimitDelayedMs());

                if (queryRes.getRetryStats() != null) {
                    RetryStats stats = queryRes.getRetryStats();
                    if (dto.totalRetryStats == null) {
                        dto.totalRetryStats = stats;
                    } else {
                        dto.totalRetryStats.addStats(stats);
                    }
                }
                if (!queryRequest.isDone()) {
                    return processQuery(queryRequest, numLimit, dto);
                }
                return CompletableFuture.completedFuture(null);
            });
    }
    private static class QueryResultDTO {
        private final LongAdder numRows = new LongAdder();
        private final LongAdder readKB = new LongAdder();
        private final LongAdder readUnits = new LongAdder();
        private final LongAdder writeKB = new LongAdder();
        private final LongAdder writeUnits = new LongAdder();
        private final LongAdder numBatches = new LongAdder();
        private final List<MapValue> items = new ArrayList<>();
        private final LongAdder totalRateLimitDelay = new LongAdder();
        private RetryStats totalRetryStats = null;
    }

    public static class TestSubscriber implements Flow.Subscriber<List<MapValue>> {

        private final List<MapValue> items =
            Collections.synchronizedList(new ArrayList<>());
        private final CompletableFuture<List<MapValue>> future =
            new CompletableFuture<>();
        private Flow.Subscription subscription;

        // Configurable request amount (defaults to unbounded)
        private final long initialRequest;

        public TestSubscriber() {
            this(Long.MAX_VALUE);
        }

        public TestSubscriber(long initialRequest) {
            this.initialRequest = initialRequest;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (initialRequest > 0) {
                subscription.request(initialRequest);
            }
        }

        @Override
        public void onNext(List<MapValue> item) {
            items.addAll(item);
        }

        @Override
        public void onError(Throwable throwable) {
            // Fail the future so the test throws the exception
            future.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            // complete the future with the final list of items
            future.complete(items);
        }

        /**
         * Blocks until the stream completes and returns the list of items received.
         * Throws an exception if the stream signaled onError.
         */
        List<MapValue> awaitItems(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        /**
         * Helper to request more items manually if you didn't request Long.MAX_VALUE initially
         */
        void request(long n) {
            if (subscription != null) {
                subscription.request(n);
            }
        }
    }
}
