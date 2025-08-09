/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;

import oracle.nosql.driver.ops.AbortTransactionRequest;
import oracle.nosql.driver.ops.BeginTransactionRequest;
import oracle.nosql.driver.ops.CommitTransactionRequest;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.Transaction;
import oracle.nosql.driver.ops.TransactionResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;

/**
 * Test interactive transaction APIs
 */
public class TransactionTest extends ProxyTestBase {
    private static final boolean verbose = false;
    private static final int DDL_WAIT_MS = 30000;

    private static final TableLimits limits = new TableLimits(500, 500, 1);
    private static final String testTable = "TransactionTest";
    private static final String tableDdl =
        "CREATE TABLE IF NOT EXISTS "+ testTable +
        "(sid INTEGER, id INTEGER, i0 INTEGER, i1 INTEGER, s STRING, " +
         "PRIMARY KEY(shard(sid), id))";

    private static final String childTable = testTable + ".child";
    private static final String childDdl =
        "CREATE TABLE IF NOT EXISTS "+ childTable +
        "(cid INTEGER, ci0 INTEGER, ci1 INTEGER, PRIMARY KEY(cid))";

    private static final String foo = "foo";
    private static final String fooDdl =
        "CREATE TABLE IF NOT EXISTS " + foo +
        "(id INTEGER, s STRING, PRIMARY KEY(id))";

    private static final Class<?> IAE = IllegalArgumentException.class;

    /**
     * A simple smoke test to verify the basic functionality of transactions.
     */
    @Test
    public void smokeTest() {
        final int sid = 0;
        final MapValue row0 = row(sid, 0);
        final MapValue key0 = primaryKey(sid, 0);
        final String queryBySid = queryBySid(testTable, sid);
        final String insert = "insert into " + testTable +
                              " values(" + sid + ", 1, 0, 0, 's1')";
        final String update = "update " + testTable +
                              " set i0 = i0 + 1 where sid = ?";
        final String select = "select * from " + testTable + " where sid = ?";
        final String delete = "delete from " + testTable + " where sid = " + sid;

        createTestTable();

        final List<Operation<?>> ops = new ArrayList<>();
        ops.add(new Put(testTable, row0, true));
        ops.add(new Get(testTable, key0, true));
        ops.add(new Delete(testTable, key0, true));
        ops.add(new WriteQuery(insert, 1));
        ops.add(new WriteQuery(delete, 1));
        ops.add(new Put(testTable, row0, true));
        ops.add(new WriteQuery(update, (bs) -> {
            bs.setVariable(1, new IntegerValue(sid));
        }, 1));
        ops.add(new Query(select, (bs) -> {
            bs.setVariable(1, new IntegerValue(sid));
        }, 1));

        /*
         * Execute these operations in a transaction, then commit the
         * transaction
         */
        executeAndCommit(ops);
        runQuery(queryBySid, 1);

        /*
         * Execute these operations in a transaction, then abort the
         * transaction
         */
        deleteAll(testTable);
        executeAndAbort(ops);
        runQuery(queryBySid, 0);
    }

    /**
     * Tests the put, get, and delete operations within a transaction.
     */
    @Test
    public void testPutGetDeletes() {
        final int sid = 0;
        final int numIdsPerSid = 3;
        final String queryBySid = queryBySid(testTable, sid);

        createTestTable();

        final List<Operation<?>> ops = new ArrayList<>();
        /*
         * Concurrent puts
         */
        for (int id = 0; id < numIdsPerSid; id++) {
            ops.add(new Put(testTable, row(sid, id), true));
        }
        /*
         * put rows with if-present option, operation should fail as the row
         * does not exist.
         */
        for (int id = numIdsPerSid; id < numIdsPerSid + 2; id++) {
            ops.add(new Put(testTable, primaryKey(sid, id),
                            Option.IfPresent, false));
        }
        /* puts and commit */
        executeAndEndTxn(ops, true /* commit */, true /* concurrent */);
        runQuery(queryBySid, numIdsPerSid);

        deleteAll(testTable);
        /* put rows and abort */
        executeAndEndTxn(ops, false /* commit */, true /* concurrent */);
        runQuery(queryBySid, 0);

        /*
         * Concurrent deletes
         */
        loadRows(2, numIdsPerSid);
        ops.clear();
        for (int id = 0; id < numIdsPerSid; id++) {
            ops.add(new Delete(testTable, primaryKey(sid, id), true));
        }
        /* delete rows that does not exist */
        for (int id = numIdsPerSid; id < numIdsPerSid + 2; id++) {
            ops.add(new Delete(testTable, primaryKey(sid, id), false));
        }
        /* delete rows and commit */
        executeAndEndTxn(ops, true/* commit */, true /* concurrent */);
        runQuery(queryBySid, 0);

        /* delete rows and abort */
        loadRows(1, numIdsPerSid);
        executeAndEndTxn(ops, false/* commit */, true /* concurrent */);
        runQuery(queryBySid, numIdsPerSid);

        /*
         * Concurrent gets
         */
        ops.clear();
        for (int id = 0; id < numIdsPerSid; id++) {
            ops.add(new Get(testTable, primaryKey(sid, id), true));
        }
        executeAndEndTxn(ops, true/* commit */, true /* concurrent */);

        /*
         * Mixed put, get, delete
         */
        ops.clear();
        deleteAll(testTable);

        MapValue key0 = primaryKey(sid, 0);
        MapValue row0 = row(sid, 0);
        MapValue row1 = row(sid, 1);

        ops.add(new Get(testTable, key0, false));
        ops.add(new Put(testTable, row0, true));
        ops.add(new Put(testTable, row1, true));
        ops.add(new Get(testTable, key0, true));
        ops.add(new Delete(testTable, key0, true));
        ops.add(new Get(testTable, key0, false));
        ops.add(new Put(testTable, row0, Option.IfPresent, false));
        ops.add(new Get(testTable, key0, false));
        ops.add(new Put(testTable, row0, Option.IfAbsent, true));
        ops.add(new Get(testTable, key0, true));
        ops.add(new Delete(testTable, key0, true));
        ops.add(new Get(testTable, key0, false));

        /* run ops and commit */
        executeAndCommit(ops);
        runQuery(queryBySid, 1);

        /* run ops and abort */
        deleteAll(testTable);

        executeAndAbort(ops);
        runQuery(queryBySid, 0);
    }

    /**
     * Tests the multi-delete operation within a transaction.
     */
    @Test
    public void testMultiDelete() {
        final int sid = 0;
        final int numIdsPerSid = 3;
        final String queryBySid = queryBySid(testTable, sid);

        createTestTable();
        loadRows(2, numIdsPerSid);

        List<Operation<?>> ops = new ArrayList<>();
        ops.add(new MultiDelete(testTable, shardKey(sid), 0, numIdsPerSid));
        ops.add(new Query(queryBySid, 0));
        executeAndCommit(ops);
        runQuery(queryBySid, 0);

        loadRows(1, numIdsPerSid);
        ops.clear();
        ops.add(new MultiDelete(testTable, shardKey(sid), 1, numIdsPerSid));
        ops.add(new Query(queryBySid, 0));
        executeAndAbort(ops);
        runQuery(queryBySid, numIdsPerSid);
    }

    /**
     * Tests the write-multiple operation within a transaction.
     */
    @Test
    public void testWriteMultiple() {
        final List<Operation<?>> ops = new ArrayList<>();
        final int sid = 0;
        final int numIdsPerSid = 3;
        final String queryBySid = queryBySid(testTable, sid);
        final String queryChildBySid = queryBySid(childTable, sid);

        createTestTable();
        createChildTable();

        loadRows(2, numIdsPerSid);
        loadChildRows(1, numIdsPerSid, 1);

        MapValue newRow0 = row(sid, 0).put("i0", 100);
        List<WriteRequest> wrops = new ArrayList<>();
        wrops.add(new PutRequest()
                    .setTableName(testTable)
                    .setValue(newRow0));
        wrops.add(new PutRequest()
                    .setTableName(testTable)
                    .setValue(row(sid, 1))
                    .setOption(Option.IfAbsent));
        wrops.add(new DeleteRequest()
                    .setTableName(testTable)
                    .setKey(primaryKey(sid, 2)));
        wrops.add(new PutRequest()
                    .setTableName(childTable)
                    .setValue(childRow(sid, 0, 1))
                    .setOption(Option.IfAbsent));
        wrops.add(new DeleteRequest()
                    .setTableName(childTable)
                    .setKey(childKey(sid, 0, 0)));
        wrops.add(new DeleteRequest()
                    .setTableName(childTable)
                    .setKey(childKey(sid, 1, 0)));
        WriteMultiple writeMultiple = new WriteMultiple(wrops,
            ret -> {
                List<OperationResult> results = ret.getResults();
                assertEquals(ops.size(), results.size());
                assertNotNull(results.get(0).getVersion());
                assertNull(results.get(1).getVersion());
                assertTrue(results.get(2).getSuccess());
                assertNotNull(results.get(3).getVersion());
                assertTrue(results.get(4).getSuccess());
                assertTrue(results.get(5).getSuccess());
            });

        ops.add(writeMultiple);
        ops.add(new Get(testTable, primaryKey(sid, 0), r -> {
            assertEquals(newRow0, r.getValue());
        }));
        ops.add(new Get(testTable, primaryKey(sid, 2), false));
        ops.add(new Get(childTable, childKey(sid, 0, 1), true));
        ops.add(new Get(childTable, childKey(sid, 0, 0), false));
        ops.add(new Get(childTable, childKey(sid, 1, 0), false));

        /* Rerun ops and commit */
        executeAndCommit(ops);
        runQuery(queryBySid, numIdsPerSid - 1);
        runQuery(queryChildBySid, numIdsPerSid - 1);

        /* Rerun ops and abort */
        deleteAll(testTable);
        deleteAll(childTable);
        loadRows(2, numIdsPerSid);
        loadChildRows(1, numIdsPerSid, 1);

        executeAndAbort(ops);
        runQuery(queryBySid, numIdsPerSid);
        runQuery(queryChildBySid, numIdsPerSid);
    }

    @Test
    /**
     * Tests mixed query operations within a transaction.
     */
    public void testQueries() {

        final int sid = 0;
        final int numIdsPerSid = 3;
        final int newId = 10;

        String selectBySid = queryBySid(testTable, 0);
        String selectOne = "select * from " + testTable +
                           " where sid = ? and id = ?";
        String insert = "insert into " + testTable + " values(?, ?, ?, ?, ?)";
        String update = "update " + testTable +
                        " set i0 = i1 + 100 " +
                        "where sid = " + sid;
        String delete = "delete from " + testTable +
                        " where sid = ? and id > ?";

        createTestTable();
        loadRows(2, numIdsPerSid);

        final List<Operation<?>> ops = new ArrayList<>();
        ops.add(new Query(selectBySid, numIdsPerSid));
        ops.add(new WriteQuery(insert,
                               ps ->  {
                                  ps.setVariable(1, new IntegerValue(sid));
                                  ps.setVariable(2, new IntegerValue(newId));
                                  ps.setVariable(3, new IntegerValue(newId));
                                  ps.setVariable(4, new IntegerValue(newId));
                                  ps.setVariable(5, new StringValue("s" + newId));
                               }, 1));
        ops.add(new Query(selectOne,
                          ps ->  {
                              ps.setVariable(1, new IntegerValue(sid));
                              ps.setVariable(2, new IntegerValue(newId));
                          }, 1));
        ops.add(new WriteQuery(update, numIdsPerSid + 1));
        ops.add(new Query(selectBySid, rs -> {
                    assertEquals(4, rs.results.size());
                    rs.results.forEach(rv -> {
                       assertEquals(rv.getInt("i0"), rv.getInt("i1") + 100);
                    });
                }));
        ops.add(new WriteQuery(delete,
                ps ->  {
                    ps.setVariable(1, new IntegerValue(0));
                    ps.setVariable(2, new IntegerValue(0));
                }, numIdsPerSid));
        ops.add(new Query(selectBySid, 1));

        /* Run ops and commit */
        executeAndCommit(ops);
        runQuery(selectBySid, 1);

        /* Run ops and abort */
        deleteAll(testTable);
        loadRows(2, numIdsPerSid);
        executeAndAbort(ops);
        runQuery(selectBySid, numIdsPerSid);
    }

    /**
     * Tests the prepare operation within a transaction.
     *
     * This test case covers various scenarios:
     * - Fail cases
     *   1. Prepare a query on a table different from the transaction's table.
     *   2. Prepare a query without shard key in where clause
     *   3. Prepare a query using a shard key different from the transaction’s
     *      shard key.
     * - Prepare won't be the binding operation
     */
    @Test
    public void testPrepare() {
        final int sid = 0;
        final int numIdsPerSid = 3;
        final String selectBySid0 = queryBySid(testTable, sid);
        final String selectBySid100 = queryBySid(testTable, 100);

        createTestTable();
        createFooTable();
        loadRows(2, numIdsPerSid);

        PreparedStatement pstmt;

        Transaction txn = beginTransaction();

        /* Prepare a query on different table, it should fail. */
        new Prepare("select * from " + foo, IAE).perform(txn);

        /*
         * Prepare a query without shard key specified in where clause, it
         * should fail.
         */
        new Prepare("select * from " + testTable, IAE).perform(txn);

        /*
         * Prepare a query with shard key [sid=100].
         * Operation succeeds, but it won't be the binding operation
         */
        pstmt = new Prepare(selectBySid100).perform(null);

        /*
         * Get operation, shard key [sid=0].
         * Operation succeeds, this is the binding operation of the transaction
         */
        new Get(testTable, primaryKey(sid, 0), true).perform(txn);

        /*
         * Execute the query prepared after the binding operation
         *
         * Operation should fail because its shard key does not match the
         * transaction’s shard key.
         */
        new Query(pstmt.getSQLText(), () -> pstmt, IAE).perform(txn);

        /*
         * Prepare the query with shard key[sid=100]
         * Operation should fail due to a shard-key mismatch between the query
         * and the transaction.
         */
        new Prepare(selectBySid100, IAE).perform(txn);

        /* Query with shard key[sid=0] */
        new Query(selectBySid0, numIdsPerSid).perform(txn);

        /* Run ops and commit */
        abortTransaction(txn);
    }

    /**
     * Tests the execution of various invalid queries within a transaction.
     * The test case covers different scenarios where queries are expected to
     * fail due to various reasons.
     */
    @Test
    public void testInvalidQueryInTxn() {
        String[] queries = new String[] {
            /* missing shard key in the WHERE clause */
            "select * from " + testTable,
            /* shard key using operators other than '=' */
            "select * from " + testTable + " where sid > 0",
            "select * from " + testTable + " where sid != 1",
            /* missing shard key in the WHERE clause */
            "select * from " + testTable + " $t where partition($t) = 1",
            /* missing shard key in the WHERE clause */
            "select count(*) from " + testTable + " group by sid",

            /* missing shard key in the WHERE clause */
            "delete from " + testTable + " where id = 0",
            /* multiple shard keys */
            "update " + testTable + " set s = 'update' || s " +
            "where sid = 1 or sid = 2",

            /* inner join, missing shard key of 1st table */
            "select * from " + testTable + " p, " + childTable + " c " +
            "where p.sid = c.sid and c.sid = 1",

            /* nested table, missing shard key of the target table */
            "select * from nested tables(" +
            testTable + " p descendants(" + childTable + " c)) " +
            "where c.sid = 1",

            /* left outer join, missing shard key of the target table */
            "select * from " +
            testTable + " p left outer join " + childTable + " c " +
            "on p.sid = c.sid and p.id = c.id " +
            "where c.sid = 1",

            /* query on a table different from transaction's table */
            "select * from foo where id = 0"
        };

        final String queryBySid = queryBySid(testTable, 0);

        createTestTable();
        createChildTable();
        createFooTable();

        final List<Operation<?>> ops = new ArrayList<>();
        ops.add(new Query(queryBySid, -1));
        for (String query : queries) {
            ops.add(new Query(query, IAE));
        }
        executeAndCommit(ops);
    }

    /*
     * Tests the execution of various invalid operations within a transaction:
     *   - The Request table is not in the hierarchy of transaction table
     *   - The shard key of this request is different from the transaction’s
     *     shard key.
     */
    @Test
    public void testInvalidOpInTxn() {

        createTestTable();
        createChildTable();
        createFooTable();

        MapValue key0 = primaryKey(0, 0);
        MapValue row0 = row(0, 0);
        MapValue key1 = primaryKey(1, 0);
        MapValue row1 = row(1, 0);
        String queryFooById = "select * from " + foo + " where id = 0";
        String queryBySid1 = queryBySid(testTable, 1);

        Transaction txn = beginTransaction();
        /*
         * The binding operation of the transaction
         *   - table:  testTable
         *   - shardKey:  sid = 0
         */
        new Get(testTable.toUpperCase(), key0, false).perform(txn);

        /*
         * The Request table is not in the hierarchy of transaction table
         */
        new Get(foo, key0, IAE).perform(txn);
        new Put(foo, key0, IAE).perform(txn);
        new Delete(foo, key0, IAE).perform(txn);
        new MultiDelete(foo, key0, IAE).perform(txn);
        new WriteMultiple(Arrays.asList(new PutRequest()
                                            .setTableName(foo)
                                            .setValue(row0),
                                        new DeleteRequest()
                                            .setTableName(foo)
                                            .setKey(key0)
                                        ),
                IAE).perform(txn);
        new Prepare(queryFooById, IAE).perform(txn);
        new Query(queryFooById, IAE).perform(txn);

        /*
         * The shard key of this request is different from the transaction’s
         * shard key.
         */
        new Get(testTable, key1, IAE).perform(txn);
        new Put(testTable, row1, IAE).perform(txn);
        new Delete(testTable, key1, IAE).perform(txn);
        new MultiDelete(testTable, key1, IAE).perform(txn);
        new WriteMultiple(Arrays.asList(new PutRequest()
                                            .setTableName(testTable)
                                            .setValue(row1),
                                        new DeleteRequest()
                                            .setTableName(testTable)
                                            .setKey(key1)),
                IAE).perform(txn);
        new Prepare(queryBySid1, IAE).perform(txn);
        new Query(queryBySid1, IAE).perform(txn);

        commitTransaction(txn);

        /*
         * Transaction has been ended
         */
        new Get(testTable.toUpperCase(), key0, IAE).perform(txn);
    }


    /**
     * Tests the behavior of a transaction when a binding operation fails.
     *
     * If the binding operation fails, it is unbound from the transaction and
     * the next eligible operation becomes the binding operation.
     *
     * In this test, a put operation with an invalid row is used, which is
     * expected to fail. The test is executed multiple times to ensure that
     * the invalid put operation can be selected as the binding operation.
     */
    @Test
    public void testBindOperationFailed() {
        final int sid = 0;
        final List<Operation<?>> ops = new ArrayList<>();

        createTestTable();

        /*
         * Put with a row where sid = 1 and the id field is missing, put should
         * fail.
         */
        MapValue badRow = row(sid + 1, 0);
        badRow.remove("id");
        ops.add(new Put(testTable, badRow, IAE));

        /* 3 more puts with row sid = 0 */
        for (int id = 0; id < 3; id++) {
            ops.add(new Put(testTable, row(sid, id), true));
        }

        for (int i = 0; i < 10; i++) {
            executeAndEndTxn(ops, true /* commit */, true /* concurrency */);
        }
    }

    /**
     * Tests the read/write cost of an operation within a transaction.
     *   - Read cost is doubled, as the read operation is executed on the
     *     master node.
     *   - Write cost remains unchanged
     */
    @Test
    public void testReadWriteCost() {
        assumeTrue("skip testReadWriteCost in onprem test", !onprem);
        createTestTable();
        testReadWriteCostInternal(true /* commit */);

        deleteAll(testTable);
        testReadWriteCostInternal(false /* commit */);
    }

    private void testReadWriteCostInternal(boolean commit) {
        final int ROW_KB = 2;
        final int READ_ROW_TXN = ROW_KB * 2;
        final int MIN_READ_TXN = 2;
        final int PREP_RU = 2;
        final String SVAL = genString((ROW_KB - 1) * 1024);

        final int sid = 0;
        final MapValue row0 = row(sid, 0, ROW_KB);
        final MapValue key0 = primaryKey(sid, 0);
        final String insert = "insert into " + testTable +
                              " values(" + sid + ", 1, 0, 0, '" + SVAL + "')";
        final String update = "update " + testTable +
                              " set i0 = i0 + 1 where sid = " + sid;
        final String select = "select * from " + testTable + " where sid = ?";

        Transaction txn = beginTransaction();

        Result ret = new Put(testTable, row0, true).perform(txn);
        assertReadWriteCost(ret, 0, ROW_KB);

        ret = new Get(testTable, key0, true).perform(txn);
        assertReadWriteCost(ret, READ_ROW_TXN, 0);

        ret = new Delete(testTable, key0, true).perform(txn);
        assertReadWriteCost(ret, MIN_READ_TXN, ROW_KB);

        ret = new Get(testTable, key0, false).perform(txn);
        assertReadWriteCost(ret, MIN_READ_TXN, 0);

        ResultSet rs = new WriteQuery(insert, 1).perform(txn);
        assertQueryCost(rs, PREP_RU + MIN_READ_TXN, ROW_KB);

        rs = new WriteQuery(update, 1).perform(txn);
        assertQueryCost(rs, PREP_RU + READ_ROW_TXN , ROW_KB * 2);

        rs = new Query(select, (bs) -> {
            bs.setVariable(1, new IntegerValue(sid));
        }, 1).perform(txn);
        assertQueryCost(rs, PREP_RU + READ_ROW_TXN, 0);

        if (commit) {
            commitTransaction(txn);
        } else {
            abortTransaction(txn);
        }
    }

    private void loadRows(int numSids, int numIdsPerSid) {
        for (int sid = 0; sid < numSids; sid++) {
            for (int id = 0; id < numIdsPerSid; id++) {
                new Put(testTable, row(sid, id), true).perform(null);
            }
        }
    }

    private void loadChildRows(int numSids,
                               int numIdsPerSid,
                               int numCidsPerId) {
        for (int sid = 0; sid < numSids; sid++) {
            for (int id = 0; id < numIdsPerSid; id++) {
                for (int cid = 0; cid < numCidsPerId; cid++) {
                    new Put(childTable, childRow(sid, id, cid), true)
                        .perform(null);
                }
            }
        }
    }

    private static String queryBySid(String tableName, int sid) {
        return "select * from " + tableName + " where sid = " + sid;
    }

    private void executeAndCommit(List<Operation<?>> ops) {
        executeAndEndTxn(ops, true /* commit */, false /* concurrent */);
    }

    private void executeAndAbort(List<Operation<?>> ops) {
        executeAndEndTxn(ops, false /* commit */, false /* concurrent */);
    }

    private void executeAndEndTxn(List<Operation<?>> ops,
                                  boolean commit,
                                  boolean concurrent) {
        final String type = (commit ? "commit" : "abort");
        trace("Run ops in a transaction: " + type + "\n---");

        Transaction txn = null;
        try {
            txn = beginTransaction();
            if (concurrent) {
                runConcurrentOps(ops, txn);
            } else {
                for (Operation<?> op : ops) {
                    op.perform(txn);
                }
            }

            if (commit) {
                commitTransaction(txn);
            } else {
                abortTransaction(txn);
            }

        } catch (Throwable t) {
            if (txn != null && txn.isActive()) {
                abortTransaction(txn);
            }
            fail(t.getMessage());
        }
    }

    private void runConcurrentOps(List<Operation<?>> ops, Transaction txn) {
        ExecutorService executor = Executors.newFixedThreadPool(ops.size());
        List<Callable<Void>> tasks = new ArrayList<>();
        ops.forEach((op) -> {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    op.perform(txn);
                    return null;
                }
            });
        });

        try {
            int i = 0;
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException ee) {
                    fail("Operation fail, op=" + ops.get(i) +
                         ", error=" + ee.getCause());
                }
                i++;
            }
        } catch (InterruptedException ie) {
            fail("Interrupted while waiting for tasks to complete");
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        System.err.println("Executor did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private Transaction beginTransaction() {
        BeginTransactionRequest txnReq = new BeginTransactionRequest()
            .setTableName(testTable);
        TransactionResult ret = handle.beginTransaction(txnReq);
        Transaction txn = ret.getTransaction();
        assertNotNull(txn);
        assertTrue(txn.isActive());
        if (!onprem) {
            assertReadWriteCost(ret, 0, 1);
        }
        trace("Begin " + txn);
        return txn;
    }

    private void commitTransaction(Transaction txn) {
        CommitTransactionRequest request = new CommitTransactionRequest()
                .setTransaction(txn);
        TransactionResult ret = handle.commitTransaction(request);
        trace("Commit OK: " + ret.getTransaction());
        assertTransactionEnded(ret);
    }

    private void abortTransaction(Transaction txn) {
        AbortTransactionRequest request = new AbortTransactionRequest()
                .setTransaction(txn);
        TransactionResult ret = handle.abortTransaction(request);
        trace("Abort OK: " + ret.getTransaction());
        assertTransactionEnded(ret);
    }

    private void assertReadWriteCost(Result ret, int ru, int wu) {
        assertEquals("Wrong readUnits: ", ru, ret.getReadUnitsInternal());
        assertEquals("Wrong writeUnits: ", wu, ret.getWriteUnitsInternal());
    }

    private void assertQueryCost(ResultSet rs, int ru, int wu) {
        assertEquals("Wrong query readUnits: ", ru, rs.readUnits);
        assertEquals("Wrong query writeUnits: ", wu, rs.writeUnits);
    }

    private void assertTransactionEnded(TransactionResult ret) {
        if (!onprem) {
            assertReadWriteCost(ret, 0, 1);
        }

        Transaction txn = ret.getTransaction();
        assertFalse(txn.isActive());

        /* TODO: verify number of elapsedTimeMs reads/writes? */
    }

    private void deleteAll(String tableName) {
        runQuery("delete from " + tableName);
    }

    private ResultSet runQuery(String query) {
        return new Query(query, -1).perform(null);
    }

    private ResultSet runQuery(String query, int numResults) {
        return new Query(query, numResults).perform(null);
    }

    private static MapValue row(int sid, int id) {
        return row(sid, id, 1);
    }

    private static MapValue row(int sid, int id, int rowKB) {
        return primaryKey(sid, id)
                .put("i0", id)
                .put("i1", id)
                .put("s", genString((rowKB - 1) * 1024));
    }

    private static MapValue primaryKey(int sid, int id) {
        return shardKey(sid)
                .put("id", id);
    }

    private static MapValue shardKey(int sid) {
        return new MapValue()
                .put("sid", sid);
    }

    private static MapValue childRow(int sid, int id, int cid) {
        return childKey(sid, id, cid)
                .put("ci0", cid)
                .put("ci1", cid);
    }

    private static MapValue childKey(int sid, int id, int cid) {
        return primaryKey(sid, id)
                .put("cid", cid);
    }

    private void createTestTable() {
        tableOperation(handle, tableDdl, limits, DDL_WAIT_MS);
    }

    private void createChildTable() {
        tableOperation(handle, childDdl, null, DDL_WAIT_MS);
    }

    private void createFooTable() {
        tableOperation(handle, fooDdl, limits, DDL_WAIT_MS);
    }

    private static void trace(String... lines) {
        if (verbose) {
            for (String line : lines) {
                System.out.println(line);
            }
        }
    }

    private class Put extends Operation<PutResult> {
        private final String tableName;
        private final MapValue row;
        private final Option option;

        Put(String tableName, MapValue row, boolean success) {
            this(tableName, row, null, success);
        }

        Put(String tableName, MapValue row, Option option, boolean success) {
            super(r -> {
                      if (success) {
                          assertNotNull("Row was not put", r.getVersion());
                      } else {
                          assertNull("Row put but should fail", r.getVersion());
                      }
                  });
            this.tableName = tableName;
            this.row = row;
            this.option = option;
        }

        Put(String tableName, MapValue row, Class<?> expectedExecption) {
            super(expectedExecption);
            this.tableName = tableName;
            this.row = row;
            this.option = null;
        }

        @Override
        PutResult execute(Transaction txn) {
            PutRequest put = new PutRequest()
                    .setTableName(tableName)
                    .setTransaction(txn)
                    .setOption(option)
                    .setValue(row);
            PutResult ret = handle.put(put);
            trace("Put " + row.toJson() +  ": " +
                  (ret.getVersion() != null ? "OK" : "FAILED!!"));
            return ret;
        }

        @Override
        public String toString() {
            return "Put " + row.toJson();
        }
    }

    private class Delete extends Operation<DeleteResult> {
        private final String tableName;
        private final MapValue key;

        Delete(String tableName, MapValue key, boolean success) {
            super(r -> {
                      if (success) {
                          assertTrue("No row deleted", r.getSuccess());
                      } else {
                          assertFalse("Row deleted but should fail",
                                      r.getSuccess());
                      }
                  });
            this.tableName = tableName;
            this.key = key;
        }

        Delete(String tableName, MapValue key, Class<?> expectedExecption) {
            super(expectedExecption);
            this.tableName = tableName;
            this.key = key;
        }

        @Override
        DeleteResult execute(Transaction txn) {
            DeleteRequest delete = new DeleteRequest()
                    .setTableName(tableName)
                    .setTransaction(txn)
                    .setKey(key);
            DeleteResult ret = handle.delete(delete);
            trace("Delete " + key.toJson() + ": " +
                  (ret.getSuccess() ? "OK" : "FAILED!!"));
            return ret;
        }

        @Override
        public String toString() {
            return "Delete [key=" + key.toJson() + "]";
        }
    }

    private class MultiDelete extends Operation<Integer> {
        private final String tableName;
        private final MapValue key;
        private final int maxWriteKB;

        MultiDelete(String tableName,
                    MapValue key,
                    int maxWriteKB,
                    int numDeletes) {
            super(r -> assertEquals("Expect to delete " + numDeletes +
                                    " rows, but actual " + r.intValue() +
                                    " rows deleted",
                                    numDeletes, r.intValue()));
            this.tableName = tableName;
            this.key = key;
            this.maxWriteKB = maxWriteKB;
        }

        MultiDelete(String tableName, MapValue key, Class<?> expectedExecption) {
            super(expectedExecption);
            this.tableName = tableName;
            this.key = key;
            this.maxWriteKB = 0;
        }

        @Override
        Integer execute(Transaction txn) {
            MultiDeleteRequest req = new MultiDeleteRequest()
                    .setTableName(tableName)
                    .setTransaction(txn)
                    .setMaxWriteKB(maxWriteKB)
                    .setKey(key);

            MultiDeleteResult ret = null;
            int numDeletes = 0;
            int wkb = 0;
            do {
                if (ret != null) {
                    req.setContinuationKey(ret.getContinuationKey());
                }

                ret = handle.multiDelete(req);
                numDeletes += ret.getNumDeletions();
                wkb += ret.getWriteKB();
            } while(ret.getContinuationKey() != null);

            trace("MultiDelete: " + key.toJson() + ", " + numDeletes +
                  " rows deleted, wkb=" + wkb);
            return numDeletes;
        }

        @Override
        public String toString() {
            return "MultiDelete [key=" + key.toJson() + "]";
        }
    }

    private class Get extends Operation<GetResult> {
        private final String tableName;
        private final MapValue key;

        Get(String tableName, MapValue key, boolean found) {
            this(tableName,
                 key,
                 r -> {
                     if (found) {
                         assertNotNull("Row not found", r.getValue());
                     } else {
                         assertNull("Row found but expect not", r.getValue());
                     }
                 });
        }

        Get(String tableName,
            MapValue key,
            Consumer<GetResult> resultHandler) {
            super(resultHandler);
            this.tableName = tableName;
            this.key = key;
        }

        Get(String tableName, MapValue key, Class<?> expectedExecption) {
            super(expectedExecption);
            this.tableName = tableName;
            this.key = key;
        }

        @Override
        GetResult execute(Transaction txn) {
            GetRequest req = new GetRequest()
                    .setTableName(tableName)
                    .setTransaction(txn)
                    .setKey(key);
            GetResult ret = handle.get(req);
            trace("Get " + key.toJson() + ": " + ret.getValue());
            return ret;
        }

        @Override
        public String toString() {
            return "Get [key=" + key.toJson() + "]";
        }
    }

    private class WriteMultiple extends Operation<WriteMultipleResult> {
        private final List<WriteRequest> ops;

        WriteMultiple(List<WriteRequest> ops,
                      Consumer<WriteMultipleResult> resultVerifier) {
            super(resultVerifier);
            this.ops = ops;
        }

        WriteMultiple(List<WriteRequest> ops, Class<?> expectedExecption) {
            super(expectedExecption);
            this.ops = ops;
        }

        @Override
        WriteMultipleResult execute(Transaction txn) {
            WriteMultipleRequest req = new WriteMultipleRequest();
            ops.forEach(op -> {
                req.add(op, false);
            });
            req.setTransaction(txn);

            WriteMultipleResult ret = handle.writeMultiple(req);
            trace("WriteMultiple: numOps=" + req.getOperations().size());
            return ret;
        }
    }

    private class Prepare extends Operation<PreparedStatement> {
        private final String query;
        Prepare(String query) {
            super((Class<?>)null);
            this.query = query;
        }

        Prepare(String query, Class<?> expectedExecption) {
            super(expectedExecption);
            this.query = query;
        }

        @Override
        public PreparedStatement execute(Transaction txn) {
            PrepareRequest req = new PrepareRequest()
                    .setStatement(query)
                    .setTransaction(txn);
            PrepareResult ret = handle.prepare(req);
            trace("Prepared " + query);
            if (txn != null) {
                assertFalse("Prepare should not be the transaction " +
                            "binding operation",
                            req.isTransactionBindingOp());
            }
            return ret.getPreparedStatement();
        }

        @Override
        public String toString() {
            return "Prepare [" + query + "]";
        }
    }

    private class WriteQuery extends Query {
        WriteQuery(String query, int numAffectedRows) {
            this(query, null, numAffectedRows);
        }

        WriteQuery(String query,
                   Consumer<PreparedStatement> bindVars,
                   int numAffectedRows) {
            this(query, bindVars, 0, numAffectedRows);
        }

        WriteQuery(String query,
                   Consumer<PreparedStatement> bindVars,
                   int maxReadKB,
                   int numAffectedRows) {
            super(query, bindVars, maxReadKB,
                  r -> {
                      assertEquals(1, r.results.size());
                      FieldValue val = r.results.get(0)
                                        .iterator().next().getValue();
                      assertEquals("Unexpected result from query: " + query,
                                   numAffectedRows, val.getInt());
                  });
        }
    }

    private class Query extends Operation<ResultSet> {
        protected final String query;
        private final Supplier<PreparedStatement> pstmtSupplier;
        private final int maxReadKB;
        private final Consumer<PreparedStatement> bindParams;

        Query(String query, int numResults) {
            this(query, null, numResults);
        }

        Query(String query,
              Consumer<PreparedStatement> bindParams,
              int numResults) {
            this(query, bindParams, 0 /* maxReadKB */, numResults);
        }

        Query(String query,
              Consumer<PreparedStatement> bindParams,
              int maxReadKB,
              int numResults) {
            this(query, bindParams, maxReadKB,
                 r -> {
                     if (numResults >= 0) {
                         assertEquals("Unexpected result from query: " + query,
                                      numResults, r.results.size());
                     }
                 });
        }

        Query(String query, Consumer<ResultSet> resultHandler) {
            this(query, null /* bindParams */, 0 /* maxReadKB */, resultHandler);
        }

        Query(String query,
              Consumer<PreparedStatement> bindParams,
              int maxReadKB,
              Consumer<ResultSet> resultHandler) {

            super(resultHandler);
            this.query = query;
            this.maxReadKB = maxReadKB;
            this.bindParams = bindParams;
            this.pstmtSupplier = null;
        }

        Query(String query, Class<?> expectedException) {
            this(query, null, expectedException);
        }

        Query(String query,
              Supplier<PreparedStatement> pstmtSupplier,
              Class<?> expectedException) {
            super(expectedException);
            this.query = query;
            this.pstmtSupplier = pstmtSupplier;
            this.maxReadKB = 0;
            this.bindParams = null;
        }

        @Override
        public ResultSet execute(Transaction txn) {
            PreparedStatement pstmt = null;
            ResultSet rs = new ResultSet();
            if ((bindParams != null)) {
                PrepareRequest prepReq = new PrepareRequest()
                        .setStatement(query)
                        .setTransaction(txn);
                PrepareResult prepRet = handle.prepare(prepReq);
                pstmt = prepRet.getPreparedStatement();
                bindParams.accept(pstmt);
                rs.readUnits = prepRet.getReadUnits();
            } else {
                if (pstmtSupplier != null) {
                    pstmt = pstmtSupplier.get();
                }
            }

            @SuppressWarnings("resource")
            QueryRequest req = new QueryRequest()
                    .setMaxReadKB(maxReadKB)
                    .setTransaction(txn);
            if (pstmt != null) {
                req.setPreparedStatement(pstmt);
            } else {
                req.setStatement(query);
            }
            do {
                QueryResult ret = handle.query(req);
                if (!ret.getResults().isEmpty()) {
                    rs.results.addAll(ret.getResults());
                }
                rs.readUnits += ret.getReadUnits();
                rs.writeUnits += ret.getWriteUnits();
            } while(!req.isDone());

            int i = 0;
            StringBuilder sb = new StringBuilder("Query: ");
            sb.append(query);
            for (MapValue rv : rs.results) {
                sb.append("\n\t").append(i++).append(":" ).append(rv.toJson());
            }
            sb.append("\n\t").append(rs.results.size()).append(" rows returned")
              .append(", ru(prep:2)=").append(rs.readUnits)
              .append(", wu=").append(rs.writeUnits);
            trace(sb.toString());
            return rs;
        }

        @Override
        public String toString() {
            return "Query [stmt=" + query + "]";
        }
    }

    private static class ResultSet {
        List<MapValue> results;
        int readUnits;
        int writeUnits;

        ResultSet() {
            results = new ArrayList<>();
        }
    }

    private static abstract class Operation<R> {
        protected final Consumer<R> resultHandler;
        private final Class<?> expectedExecption;

        Operation(Consumer<R> resultHandler) {
            this.resultHandler = resultHandler;
            this.expectedExecption = null;
        }

        Operation(Class<?> expectedExecption) {
            this.expectedExecption = expectedExecption;
            this.resultHandler = null;
        }

        abstract R execute(Transaction txn);

        R perform(Transaction txn) {
            try {
                R ret = execute(txn);
                if (expectedExecption != null) {
                    fail("Expected to fail, but succeeded");
                }
                if (resultHandler != null) {
                    resultHandler.accept(ret);
                }
                return ret;
            } catch (Throwable t) {
                trace(this + " failed: " + t.getMessage());
                if (expectedExecption != null) {
                    assertTrue(expectedExecption.isInstance(t));
                } else {
                    fail("Unexpected exception: operation should have " +
                         "succeeded: " + t);
                }
            }
            return null;
        }
    }
}
