/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.BinaryProtocol.BATCH_OP_NUMBER_LIMIT;
import static oracle.nosql.driver.util.BinaryProtocol.ROW_SIZE_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.nosql.driver.BatchOperationNumberLimitException;
import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

/**
 * Test on WriteMultiple operation.
 */
public class WriteMultipleTest extends ProxyTestBase {

    final static String tableName = "writeMultipleTable";
    final static String childTableName = "writeMultipleTable.child";

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS writeMultipleTable(" +
        "sid INTEGER, id INTEGER, name STRING, longString STRING, " +
        "PRIMARY KEY(SHARD(sid), id)) " +
        "USING TTL 1 DAYS";

    /* a single child table, if supported by server */
    final static String createChildTableDDL =
        "CREATE TABLE IF NOT EXISTS writeMultipleTable.child(" +
        "childid INTEGER, childname STRING, childdata STRING, " +
        "PRIMARY KEY(childid)) " +
        "USING TTL 1 DAYS";

    final static TimeToLive tableTTL = TimeToLive.ofDays(1);

    /* child tables first appeared in KV 21.2 */
    final static int childTablesKVVersion = 21_002_000;

    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();
        tableOperation(handle, createTableDDL,
                       new TableLimits(5000, 5000, 50));
        if (kvServerVersion >= childTablesKVVersion) {
            tableOperation(handle, createChildTableDDL, null);
        }
    }

    @Override
    public void afterTest() throws Exception {
        if (kvServerVersion >= childTablesKVVersion) {
            dropTable(childTableName);
        }
        dropTable(tableName);
        super.afterTest();
    }

    /**
     * Test operation succeed.
     */
    @Test
    public void testOpSucceed() {

        final int sid = 10;
        final int recordKB = 2;
        WriteMultipleRequest umRequest = new WriteMultipleRequest();
        List<Boolean> shouldSucceed = new ArrayList<Boolean>();
        List<Boolean> rowPresent = new ArrayList<Boolean>();

        /* Put 10 rows */
        for (int i = 0; i < 10; i++) {
            MapValue value = genRow(sid, i, recordKB);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);
            umRequest.add(putRequest, false);
            rowPresent.add(false);
            shouldSucceed.add(true);
        }

        WriteMultipleResult umResult = handle.writeMultiple(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, rowPresent, recordKB);
        Version versionId2 = umResult.getResults().get(2).getVersion();
        Version versionId7 = umResult.getResults().get(7).getVersion();

        umRequest.clear();
        shouldSucceed.clear();
        rowPresent.clear();

        /* PutIfAbsent, ReturnRow = true */
        MapValue value = genRow(sid, 0, recordKB, true);
        PutRequest put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(false);

        /* PutIfPresent, ReturnRow = true */
        value = genRow(sid, 1, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* PutIfVersion, ReturnRow = true */
        value = genRow(sid, 2, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(versionId2)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* PutIfAbsent, ReturnRow = false */
        value = genRow(sid, 10, recordKB, true);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(put, false);
        rowPresent.add(false);
        shouldSucceed.add(true);

        /* Put, ReturnRow = true */
        value = genRow(sid, 3, recordKB, true);
        put = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* Put, ReturnRow = false */
        value = genRow(sid, 4, recordKB, true);
        put = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(put, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* Delete, ReturnRow = true */
        value = genKey(sid, 5);
        DeleteRequest delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* Delete, ReturnRow = false */
        value = genKey(sid, 6);
        delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(false);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* DeleteIfVersion, ReturnRow = true */
        value = genKey(sid, 7);
        delete = new DeleteRequest()
            .setMatchVersion(versionId7)
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(true);

        /* DeleteIfVersion, ReturnRow = true */
        value = genKey(sid, 8);
        delete = new DeleteRequest()
            .setMatchVersion(versionId7)
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(true);
        shouldSucceed.add(false);

        /* Delete, ReturnRow = true */
        value = genKey(sid, 100);
        delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);
        rowPresent.add(false);
        shouldSucceed.add(false);

        umResult = handle.writeMultiple(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, rowPresent, recordKB);

        if (kvServerVersion < childTablesKVVersion) {
            return;
        }

        umRequest.clear();
        shouldSucceed.clear();
        rowPresent.clear();

        /* Put rows with intermixed parent and child table data */
        for (int i = 0; i < 5; i++) {
            value = genRow(20, i, recordKB);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);
            umRequest.add(putRequest, false);
            rowPresent.add(false);
            shouldSucceed.add(true);
            value = genChildRow(20, i, i, "name_" + i, "data_" + i);
            putRequest = new PutRequest()
                .setValue(value)
                .setTableName(childTableName);
            umRequest.add(putRequest, false);
            rowPresent.add(false);
            shouldSucceed.add(true);
        }

        try {
            umResult = handle.writeMultiple(umRequest);
        } catch (OperationNotSupportedException onse) {
            /*
             * This is expected in:
             * 21.2 <= .51
             * 22.1 <= .22
             * 22.2 <= .13
             */
           if (kvServerVersion <= 21_002_051 ||
               (kvServerVersion >= 22_001_000 && kvServerVersion <= 22_001_022) ||
               (kvServerVersion >= 22_002_000 && kvServerVersion <= 22_002_013) ||
               (kvServerVersion >= 22_003_000 && kvServerVersion <= 22_003_002)) {
               return;
           }
           throw onse;
        } catch (TableNotFoundException tnfe) {
           /* expected in 22.3.2 only */
           if (kvServerVersion == 22_003_002) {
               return;
           }
           throw tnfe;
        }

        verifyResult(umResult, umRequest, shouldSucceed, rowPresent, recordKB);
    }

    /**
     * Test operation aborted. The whole update multiple operation is aborted
     * due to the failure of an operation with abortIfSuccessful set to true.
     */
    @Test
    public void testOpAborted() {

        final int sid = 20;
        final int recordKB = 2;

        /*
         * The whole writeMultiple operation aborted due to a failure of
         * 2nd operation.
         */
        MapValue oldVal101 = genRow(sid, 101, recordKB);
        PutRequest req = new PutRequest()
            .setValue(oldVal101)
            .setTableName(tableName);
        PutResult ret = handle.put(req);
        Version oldVer101 = ret.getVersion();
        assert(oldVer101 != null);

        MapValue newVal101 = genRow(sid, 101, recordKB);
        newVal101.put("name", newVal101.get("name").getString() + "_upd");
        req.setValue(newVal101);
        ret = handle.put(req);
        Version newVer101 = ret.getVersion();
        assert(newVer101 != null);

        PutRequest reqOK = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(genRow(sid, 100, recordKB))
            .setTableName(tableName);

        PutRequest reqNotExec = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(genRow(sid, 200, recordKB))
            .setTableName(tableName);

        boolean[] rowPresents = new boolean[] {false /* for reqOK */,
                                               false /* for reqFail */,
                                               false /* for reqNotExec */};

        /* 3 operations, fail at 2nd operation: PutIfAbsent */
        WriteRequest reqFail = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(genRow(sid, 101, recordKB))
            .setTableName(tableName)
            .setReturnRow(true);

        rowPresents[1] = true;
        runOpAbortedTest(reqOK, reqFail, reqNotExec, recordKB,
                         rowPresents, newVer101, newVal101);

        /* 3 operations, fail at 2nd operation: PutIfPresent */

        reqFail = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(genRow(sid, 102, recordKB))
            .setTableName(tableName)
            .setReturnRow(true);

        rowPresents[1] = false;
        runOpAbortedTest(reqOK, reqFail, reqNotExec, recordKB,
                         rowPresents, null, null);

        /* 3 operations, fail at 2nd operation: PutIfVersion */
        reqFail = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(oldVer101)
            .setValue(genRow(sid, 101, recordKB))
            .setTableName(tableName)
            .setReturnRow(true);

        rowPresents[1] = true;
        runOpAbortedTest(reqOK, reqFail, reqNotExec, recordKB,
                         rowPresents, newVer101, newVal101);

        /* 3 operations, fail at 2nd operation: Delete */
        reqFail = new DeleteRequest()
            .setKey(genKey(sid, 102))
            .setTableName(tableName)
            .setReturnRow(true);

        rowPresents[1] = false;
        runOpAbortedTest(reqOK, reqFail, reqNotExec, recordKB,
                         rowPresents, null, null);

        /* 3 operations, fail at 2nd operation: DeleteIfVersion */
        reqFail = new DeleteRequest()
            .setMatchVersion(oldVer101)
            .setKey(genKey(sid, 101))
            .setTableName(tableName)
            .setReturnRow(true);

        rowPresents[1] = true;
        runOpAbortedTest(reqOK, reqFail, reqNotExec, recordKB,
                         rowPresents, newVer101, newVal101);
    }

    private void runOpAbortedTest(WriteRequest reqOK,
                                  WriteRequest reqFail,
                                  WriteRequest reqNotExec,
                                  int recordKB,
                                  boolean[] rowPresents,
                                  Version expFailOpPrevVersion,
                                  MapValue expFailOpPrevValue) {
        WriteMultipleRequest umRequest = new WriteMultipleRequest();
        WriteMultipleResult umResult;

        /* 1st op: reqOK */
        umRequest.add(reqOK, true);

        /* 2nd op: reqFail */
        int failedOpIndex = umRequest.getNumOperations();
        umRequest.add(reqFail, true);

        /* 3rd op: reqNotExec */
        umRequest.add(reqNotExec, true);

        umResult = handle.writeMultiple(umRequest);
        verifyResultAborted(umRequest, umResult, failedOpIndex, rowPresents,
                            recordKB, expFailOpPrevVersion, expFailOpPrevValue);
    }

    /**
     * Test operation failed because of invalid arguments.
     */
    @Test
    public void testOpFailed() {

        final int sid = 30;
        final int recordKB = 2;
        WriteMultipleRequest umRequest = new WriteMultipleRequest();

        /**
         * WriteMultiple operation failed due to invalid arguments
         */
        /* case1: Only put or delete request are allowed */
        umRequest.clear();
        GetRequest get = new GetRequest()
            .setKey(genKey(sid, 1))
            .setTableName(tableName);
        try {
            umRequest.add(get, false);
            fail("Expected to fail but not");
        } catch(IllegalArgumentException iae) {
        }

        /* case2: Two operations have different shard keys */
        umRequest.clear();
        /* PutIfAbsent, return row = true*/
        MapValue value = genRow(sid, 0, recordKB);
        PutRequest put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);

        /* PutIfAbsent, return row = true*/
        value = genRow(sid + 1, 0, recordKB);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);
        try {
            handle.writeMultiple(umRequest);
            fail("Expected to fail but not");
        } catch (IllegalArgumentException iae) {
        }

        /* case3: More than one operation has the same Key */
        umRequest.clear();
        /* PutIfAbsent, return row = true*/
        value = genRow(sid, 0, recordKB);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);

        /* PutIfAbsent, return row = true*/
        value = genKey(sid, 0);
        DeleteRequest delete = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(delete, false);

        try {
            handle.writeMultiple(umRequest);
            fail("Expected to fail but not");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * case4: the target table of a operation is different from that of
         * others.
         */
        umRequest.clear();
        /* PutIfAbsent, return row = true*/
        value = genRow(sid, 0, recordKB);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true);
        umRequest.add(put, false);

        /* PutIfAbsent, return row = true*/
        value = genRow(sid, 1, recordKB);
        put = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setTableName("test")
            .setReturnRow(true);
        try {
            umRequest.add(put, false);
            fail("Expected to fail but not");
        } catch (IllegalArgumentException iae) {
        }

        if (!onprem) {
            /* case5: the number of operations exceeds the limit */
            umRequest.clear();
            for (int i = 0; i < BATCH_OP_NUMBER_LIMIT +1; i++) {
                value = genRow(sid, i, recordKB);
                put = new PutRequest()
                    .setOption(Option.IfAbsent)
                    .setValue(value)
                    .setTableName(tableName)
                    .setReturnRow(true);
                umRequest.add(put, false);
            }
            try {
                handle.writeMultiple(umRequest);
                fail("Expected to fail but not");
            } catch (BatchOperationNumberLimitException ex) {
            }
        }

        if (!onprem) {
            /*
             * Case6: the data size of 2nd operation exceeds the limit.
             */
            umRequest.clear();
            value = genRow(sid, 100, recordKB);
            put = new PutRequest()
                .setOption(Option.IfAbsent)
                .setValue(value)
                .setTableName(tableName)
                .setReturnRow(true);
            umRequest.add(put, false);

            value = new MapValue().put("sid", sid).put("id", 101)
                .put("name", genString(ROW_SIZE_LIMIT));
            put = new PutRequest()
                .setValue(value)
                .setTableName(tableName)
                .setReturnRow(true);
            umRequest.add(put, false);
            try {
                handle.writeMultiple(umRequest);
                fail("Expected to fail but not");
            } catch (RowSizeLimitException rsle) {
            }
        }
    }

    @Test
    public void testOpWithTTL() {
        final int sid = 11;
        final int num = 10;
        final int recordKB = 2;

        /*
         * Test TimeToLive.fromExpirationTime, the duration between
         * referenceTime and expirationTime is less than one hour.
         */
        TimeToLive ttl = TimeToLive.fromExpirationTime(1527483600000L,
                                                       1527481131705L);
        assertEquals(ttl.getValue(), 1);
        assertEquals(ttl.getUnit(), TimeUnit.HOURS);
        /*
         * Test TimeToLive.fromExpirationTime, the duration between
         * referenceTime and expirationTime is less than one day.
         */
        ttl = TimeToLive.fromExpirationTime(1527552000000L, 1527481131705L);
        assertEquals(ttl.getValue(), 1);
        assertEquals(ttl.getUnit(), TimeUnit.DAYS);

        /* Test WriteMultipleRequest with TTL */
        WriteMultipleRequest umRequest = new WriteMultipleRequest();
        List<Boolean> shouldSucceed = new ArrayList<Boolean>();

        /* Put rows with TTL of DO_NOT_EXPIRE */
        ttl = TimeToLive.DO_NOT_EXPIRE;
        for (int i = 0; i < num; i++) {
            MapValue value = genRow(sid, i, recordKB);
            PutRequest putReq = new PutRequest()
                .setValue(value)
                .setTableName(tableName)
                .setReturnRow(false)
                .setTTL(ttl);
            umRequest.add(putReq, false);
            shouldSucceed.add(true);
        }

        WriteMultipleResult umResult = handle.writeMultiple(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, null, recordKB);

        /* Verify expiration time */
        long oldExpirationTime = 0;
        for (int i = 0; i < num; i++) {
            MapValue key = genKey(sid, i);
            GetRequest getReq = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            GetResult getRet = handle.get(getReq);
            assertTimeToLive(ttl, getRet.getExpirationTime(), 0);
            if (oldExpirationTime == 0) {
                oldExpirationTime = getRet.getExpirationTime();
            }
        }

        /* Update rows with new TTL */
        umRequest.clear();
        shouldSucceed.clear();
        umRequest.add(new DeleteRequest()
                      .setKey(genKey(sid, 100))
                      .setTableName(tableName), false);
        shouldSucceed.add(false);
        for (int i = 0; i < num; i++) {
            MapValue value = genRow(sid, i, recordKB, true);
            PutRequest putReq = new PutRequest()
                .setValue(value)
                .setTableName(tableName)
                .setReturnRow(false);
            ttl = genTTL(i);
            if (ttl == tableTTL) {
                putReq.setUseTableDefaultTTL(true);
            } else {
                putReq.setTTL(ttl);
            }
            umRequest.add(putReq, false);
            shouldSucceed.add(true);
        }

        umResult = handle.writeMultiple(umRequest);
        verifyResult(umResult, umRequest, shouldSucceed, null, recordKB);

        /* Verify expiration time */
        for (int i = 0; i < num; i++) {
            GetRequest getReq = new GetRequest()
                .setKey(genKey(sid, i))
                .setTableName(tableName);
            GetResult getRet = handle.get(getReq);
            assertTimeToLive(genTTL(i), getRet.getExpirationTime(),
                             oldExpirationTime);
        }
    }

    @Test
    public void testReturnRow() {
        final int num = 5;
        WriteMultipleResult ret;
        Version[] orgVersions = new Version[num];
        Version[] updVersions = new Version[num];

        /*
         * Execute 5 PutIfPresent ops with returnRow = true,
         * all ops fail, previous rows returned are all null.
         */
        WriteMultipleRequest reqPutIfPresents = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue mv = genRow(1, i, 1, true);
            PutRequest put = new PutRequest()
                    .setTableName(tableName)
                    .setValue(mv)
                    .setOption(Option.IfPresent)
                    .setReturnRow(true);
            reqPutIfPresents.add(put, false);
        }
        ret = handle.writeMultiple(reqPutIfPresents);
        for (OperationResult opRet: ret.getResults()) {
            assertFalse(opRet.getSuccess());
            assertNull(opRet.getExistingValue());
            assertNull(opRet.getExistingVersion());
        }

        /*
         * Execute 5 Put ops with returnRow = true,
         * all ops succeed, previous rows returned are all null.
         */
        WriteMultipleRequest reqPuts = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue mv = genRow(1, i, 1);
            PutRequest put = new PutRequest()
                    .setTableName(tableName)
                    .setValue(mv)
                    .setReturnRow(true);
            reqPuts.add(put, false);
        }
        ret = handle.writeMultiple(reqPuts);
        for (int i = 0; i < ret.getResults().size(); i++) {
            OperationResult opRet = ret.getResults().get(i);
            assertTrue(opRet.getSuccess());
            assertNull(opRet.getExistingValue());
            assertNull(opRet.getExistingVersion());

            assertNotNull(opRet.getVersion());
            orgVersions[i] = opRet.getVersion();
        }

        /*
         * Execute 5 PutIfPresent ops with returnRow = true
         * all ops succeed, previous rows returned are all null.
         */
        ret = handle.writeMultiple(reqPutIfPresents);
        for (int i = 0; i < ret.getResults().size(); i++) {
            OperationResult opRet = ret.getResults().get(i);
            assertTrue(opRet.getSuccess());
            assertNull(opRet.getExistingValue());
            assertNull(opRet.getExistingVersion());

            assertNotNull(opRet.getVersion());
            updVersions[i] = opRet.getVersion();
        }

        /*
         * Execute 5 PutIfAbsent ops with returnRow = true,
         * all ops fail, previous rows returned are not null.
         */
        WriteMultipleRequest reqPutIfAbsents = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue mv = genRow(1, i, 1);
            PutRequest put = new PutRequest()
                    .setTableName(tableName)
                    .setValue(mv)
                    .setOption(Option.IfAbsent)
                    .setReturnRow(true);
            reqPutIfAbsents.add(put, false);
        }
        ret = handle.writeMultiple(reqPutIfAbsents);
        for (int i = 0; i < ret.getResults().size(); i++) {
            OperationResult opRet = ret.getResults().get(i);
            assertFalse(opRet.getSuccess());
            assertNotNull(opRet.getExistingValue());

            assertNotNull(opRet.getExistingVersion());
            assertTrue(Arrays.equals(updVersions[i].getBytes(),
                                     opRet.getExistingVersion().getBytes()));
        }

        /*
         * Execute 5 PutIfVersion ops with returnRow = true,
         * all ops fail, previous rows returned are not null.
         */
        WriteMultipleRequest reqPutIfVersions = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue mv = genRow(1, i, 1);
            PutRequest put = new PutRequest()
                    .setTableName(tableName)
                    .setValue(mv)
                    .setMatchVersion(orgVersions[i])
                    .setOption(Option.IfVersion)
                    .setReturnRow(true);
            reqPutIfVersions.add(put, false);
        }
        ret = handle.writeMultiple(reqPutIfVersions);
        for (int i = 0; i < ret.getResults().size(); i++) {
            OperationResult opRet = ret.getResults().get(i);
            assertFalse(opRet.getSuccess());
            assertNotNull(opRet.getExistingValue());

            assertNotNull(opRet.getExistingVersion());
            assertTrue(Arrays.equals(updVersions[i].getBytes(),
                                     opRet.getExistingVersion().getBytes()));
        }

        /*
         * Execute 5 DeleteIfVersion ops with returnRow = true,
         * all ops fail, previous rows returned are not null.
         */
        WriteMultipleRequest reqDeleteIfVersions = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue key = genKey(1, i);
            DeleteRequest delete = new DeleteRequest()
                    .setTableName(tableName)
                    .setKey(key)
                    .setMatchVersion(orgVersions[i])
                    .setReturnRow(true);
            reqDeleteIfVersions.add(delete, false);
        }
        ret = handle.writeMultiple(reqPutIfVersions);
        for (int i = 0; i < ret.getResults().size(); i++) {
            OperationResult opRet = ret.getResults().get(i);
            assertFalse(opRet.getSuccess());
            assertNotNull(opRet.getExistingValue());

            assertNotNull(opRet.getExistingVersion());
            assertTrue(Arrays.equals(updVersions[i].getBytes(),
                                     opRet.getExistingVersion().getBytes()));
        }

        /*
         * Execute 5 Deletes ops with returnRow = true,
         * all ops fail, previous rows returned are all null.
         */
        WriteMultipleRequest reqDeletes = new WriteMultipleRequest();
        for (int i = 0; i < num; i++) {
            MapValue key = genKey(1, i);
            DeleteRequest delete = new DeleteRequest()
                    .setTableName(tableName)
                    .setKey(key)
                    .setReturnRow(true);
            reqDeletes.add(delete, false);
        }
        ret = handle.writeMultiple(reqDeletes);
        for (OperationResult opRet: ret.getResults()) {
            assertTrue(opRet.getSuccess());
            assertNull(opRet.getExistingValue());
            assertNull(opRet.getExistingVersion());
        }
    }

    private MapValue genRow(int sid, int id, int recordKB) {
        return genRow(sid, id, recordKB, false);
    }

    private MapValue genRow(int sid, int id, int recordKB, boolean upd) {
        return new MapValue().put("sid", sid).put("id", id)
            .put("name", (upd ? "name_upd_" : "name_") + sid + "_" + id)
            .put("longString", genString((recordKB - 1) * 1024));
    }

    private MapValue genChildRow(int sid, int id, int childid,
                                 String name, String data) {
        return new MapValue().put("sid", sid).put("id", id)
            .put("childid", id)
            .put("childname", name)
            .put("childdata", data);
    }

    private MapValue genKey(int sid, int id) {
        return new MapValue().put("sid", sid).put("id", id);
    }

    private void verifyResult(WriteMultipleResult umResult,
                              WriteMultipleRequest umRequest,
                              List<Boolean> shouldSucceedList,
                              List<Boolean> rowPresentList,
                              int recordKB) {

        assertTrue("The operation is expected to success but not",
                   umResult.getSuccess());

        List<OperationRequest> ops = umRequest.getOperations();
        assertTrue("Wrong number of results: expect " +
                   umRequest.getNumOperations() + ", actual " + umResult.size(),
                   umResult.size() == umRequest.getNumOperations());

        int ind = 0;

        for (OperationResult result : umResult.getResults()) {
            boolean shouldSucceed = shouldSucceedList.get(ind);
            assertTrue("Operation is expected to success but not, opIdx=" + ind,
                       result.getSuccess() == shouldSucceed);

            OperationRequest op = ops.get(ind);
            WriteRequest request = op.getRequest();
            if (request instanceof PutRequest && shouldSucceed) {
                assertTrue("Expected to get new version but not",
                           result.getVersion() != null);
            } else {
                assertTrue("Expected to not get new version but get",
                           result.getVersion() == null);
            }

            if (rowPresentList != null) {
                boolean rowPresent = rowPresentList.get(ind);
                boolean hasReturnRow = !result.getSuccess() &&
                                        request.getReturnRowInternal() &&
                                        rowPresent;

                assertTrue("The existing value is expected to be " +
                           (hasReturnRow ? "not null" : "null") + " but not",
                           (hasReturnRow ? result.getExistingValue() != null :
                            result.getExistingValue() == null));
                assertTrue("The existing version is expected to be " +
                           (hasReturnRow ? "not null" : "null") + " but not",
                           (hasReturnRow ? result.getExistingVersion() != null :
                            result.getExistingVersion() == null));
            }
            ind++;
        }
    }

    private void verifyResultAborted(WriteMultipleRequest request,
                                     WriteMultipleResult result,
                                     int failedOpIndex,
                                     boolean[] rowPresents,
                                     int recordKB,
                                     Version expPrevVersion,
                                     MapValue expPrevValue) {

        assertTrue("The operation is expected to abort",
                   result.getSuccess() == false);

        assertTrue("Wronng failed operation index, expect " + failedOpIndex +
                   "but get " + result.getFailedOperationIndex(),
                   result.getFailedOperationIndex() == failedOpIndex);

        Request opReq = request.getRequest(failedOpIndex);
        assertNotNull(opReq);
        OperationResult opRet = result.getFailedOperationResult();

        assertTrue("The failed operation result should not be null",
                   opRet != null);

        assertTrue("The version is expected to be null but not",
                   opRet.getVersion() == null);

        if (expPrevVersion != null) {
            assertNotNull(expPrevValue);

            assertTrue("The existing version should be not null",
                       opRet.getExistingVersion() != null);
            assertTrue("The existing version is wrong",
                       Arrays.equals(expPrevVersion.getBytes(),
                                     opRet.getExistingVersion().getBytes()));
        } else {
            assertTrue("The existing version should be null",
                       opRet.getExistingVersion() == null);
        }

        if (expPrevValue != null) {
            assertNotNull(expPrevVersion);
            assertTrue("The existing value should be not null",
                       opRet.getExistingValue() != null);
            assertTrue("The existing value is wrong",
                       expPrevValue.equals(opRet.getExistingValue()));
        } else {
            assertTrue("The existing value should be null",
                       opRet.getExistingValue() == null);
        }
    }

    private TimeToLive genTTL(int id) {
        switch (id % 4) {
        case 1:
            return TimeToLive.ofDays(id + 1);
        case 2:
            return TimeToLive.ofHours(id + 1);
        case 3:
            return tableTTL;
        case 0:
            break;
        }
        return null;
    }

    private void assertTimeToLive(TimeToLive ttl,
                                  long actExTime,
                                  long origExTime) {

        final long HOUR_IN_MILLIS = 60 * 60 * 1000;
        final long DAY_IN_MILLIS = 24 * HOUR_IN_MILLIS;

        if (ttl == null || ttl.getValue() == 0) {
            assertTrue("Expiration time should be " + origExTime + ": " +
                       actExTime, actExTime == origExTime);
        } else {
            assertTrue("Expiration time should be greater than 0",
                       actExTime > 0);

            boolean unitIsHour = ttl.unitIsHours();
            long unitInMs = (unitIsHour ? HOUR_IN_MILLIS : DAY_IN_MILLIS);
            long expExTime = ttl.toExpirationTime(System.currentTimeMillis());

            assertTrue("Actual TTL duration " + actExTime + "ms differs by " +
                       "more than a " + (unitIsHour ? "hour" : "day")  +
                       "from expected duration of " + expExTime +"ms: " + ttl,
                       Math.abs(actExTime - expExTime) <= unitInMs);
        }
    }
}
