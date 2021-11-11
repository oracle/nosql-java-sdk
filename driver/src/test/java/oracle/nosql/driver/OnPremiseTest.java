/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.BinaryProtocol.BATCH_OP_NUMBER_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Test features that are supported on-premise only at this time:
 * o child tables
 * o namespaces
 * o (TBD) security-relevant on-premise operations
 */
public class OnPremiseTest extends ProxyTestBase {

    HashSet<String> existingUsers;
    HashSet<String> existingNamespaces;

    @Override
    public void beforeTest() throws Exception {
        super.beforeTest();

        if (secure) {
            existingUsers = new HashSet<String>();

            UserInfo[] uInfo = handle.listUsers();
            if (uInfo != null) {
                for (UserInfo u : uInfo) {
                    existingUsers.add(u.getName());
                }
            }
            existingUsers.add(user);
            existingUsers.add("admin");
        }

        existingNamespaces = new HashSet<String>();
        String[] namespaces = handle.listNamespaces();
        if (namespaces != null) {
            for (String ns : namespaces) {
                existingNamespaces.add(ns);
            }
        }
    }

    /**
     * clean up tables and close handle
     */
    @After
    public void afterTest() throws Exception {

        if (handle != null) {
            dropAllMetadata(handle);
        }
        super.afterTest();
    }

    /**
     * A method to drop tables, namespaces, and users
     */
    protected void dropAllMetadata(NoSQLHandle nosqlHandle) {
        dropAllNamespaces(nosqlHandle); // this uses cascade to drop tables
        dropAllUsers(nosqlHandle);
        dropRegions(nosqlHandle);
    }

    protected void dropAllNamespaces(NoSQLHandle nosqlHandle) {
        String[] namespaces = nosqlHandle.listNamespaces();
        if (namespaces == null) {
            return;
        }

        for (String ns : namespaces) {
            if (ns.equals("sysdefault") ||
                existingNamespaces.contains(ns)) {
                continue;
            }
            /* use cascade to remove tables in namespaces */
            String statement ="drop namespace " + ns + " cascade";
            doSysOp(nosqlHandle, statement);
        }
    }

    protected void dropAllUsers(NoSQLHandle nosqlHandle) {

        if (!secure) {
            return;
        }
        UserInfo[] uInfo = nosqlHandle.listUsers();
        if (uInfo == null) {
            return;
        }
        for (UserInfo u : uInfo) {
            if (existingUsers.contains(u.getName())) {
                continue;
            }
            String statement ="drop user " + u.getName();
            doSysOp(nosqlHandle, statement);
        }
    }

    /*
     * JSON format looks like this -- an array of region objects
      {"regions" :
         [
           {"name" : "localRegion", "type" : "local", "state" : "active"},
           {"name" : "remoteRegion", "type" : "remote", "state" : "active"}
         ]
      }
    */
    protected void dropRegions(NoSQLHandle nosqlHandle) {
        SystemResult res = doSysOp(nosqlHandle, "show as json regions");
        String regionString = res.getResultString();
        if (regionString != null) {
            MapValue regions =
                FieldValue.createFromJson(regionString, null).asMap();
            ArrayValue regionArray = regions.get("regions").asArray();
            for (FieldValue region : regionArray) {
                String type = region.asMap().get("type").asString().getValue();
                if (type.equals("local")) {
                    continue;
                }
                /* dropped regions still show up, just as "dropped" */
                String state =
                    region.asMap().get("state").asString().getValue();
                if (state.equals("dropped")) {
                    continue;
                }
                String drop = "drop region " +
                    region.asMap().get("name").asString().getValue();
                doSysOp(nosqlHandle, drop);
            }
        }
    }

    /*
     * Test data limits:
     *  key size
     *  index key size
     *  row size
     * Createa  row that exceeds the cloud limits and ensure that it
     * succeeds.
     */
    @Test
    public void testDataLimits()
        throws Exception {

        final String tableName = "limits";
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id STRING, " +
            " idx STRING, " +
            " name STRING, " +
            " PRIMARY KEY(id))";
        final String addIndex = "create index idx on limits(idx)";

        tableOperation(handle, createTableStatement, null);
        tableOperation(handle, addIndex, null);

        /*
         * PUT a row that exceeds cloud key and value limits.
         */
        MapValue value = new MapValue().put("id", genString(500))
            .put("idx", genString(300)).put("name", genString(600000));

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = handle.put(putRequest);
        assertNotNull(putResult.getVersion());
    }

    /*
     * Test table limits:
     *  # of indexes. The cloud defaults to 5 so success without an exception
     * means the test passes.
     */
    @Test
    public void testTableLimits()
        throws Exception {

        final String createTableStatement =
            "create table if not exists limits(" +
            "id integer, i0 integer, i1 integer, i2 integer, i3 integer, " +
            "i4 integer, i5 integer, i6 integer, primary key(id))";

        tableOperation(handle, createTableStatement, null);

        for (int i = 0; i < 7; i++) {
            String addIndex = "create index idx" + i + " on limits(i" +
                i + ")";

            tableOperation(handle, addIndex, null);
        }
    }

    @Test
    public void testChildTables()
        throws Exception {

        String tableName = "parent";
        String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id INTEGER, " +
            " pin INTEGER, " +
            " name STRING, " +
            " PRIMARY KEY(SHARD(pin), id))";

        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = handle.tableRequest(tableRequest);
        tres.waitForCompletion(handle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        tableName = "parent.child";
        createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(childId INTEGER, " +
            " childName STRING, " +
            " PRIMARY KEY(childId))";

        tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        tres = handle.tableRequest(tableRequest);
        tres.waitForCompletion(handle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        /*
         * PUT a row
         */
        MapValue value = new MapValue().put("id", 1).
            put("pin", "654321").put("name", "test1").
            put("childId", 1).put("childName", "cName");

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = handle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the row
         */
        MapValue key = new MapValue().put("id", 1).put("pin", "654321").
            put("childId", 1);

        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getRequest);
        assertEquals("cName",
                     getRes.getValue().get("childName").
                     asString().getValue());

        /*
         * PUT a second row using JSON
         */
        String jsonString =
            "{\"id\": 2, \"pin\": 123456, \"name\":\"test2\"," +
            "\"childId\": 2, \"childName\":\"cName2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null)
            .setTableName(tableName);
        putResult = handle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * GET the second row
         */
        key = new MapValue().put("id", 2).put("pin", "123456").
            put("childId", 2);
        getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        assertEquals(
            "cName2",
            handle.get(getRequest).getValue().get("childName").
            asString().getValue());

        /*
         * QUERY the table. The table name is inferred from the
         * query statement.
         */
        try {
            QueryRequest queryRequest = new QueryRequest().
                setStatement("SELECT * from " + tableName +
                             " WHERE childName= \"cName2\"");
            QueryResult qres = handle.query(queryRequest);
            List<MapValue> results = qres.getResults();
            assertEquals(results.size(), 1);
            assertEquals(2, results.get(0).get("id").asInteger().getValue());
        } catch (RequestTimeoutException rte) {
            /*
            if (!(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            */
            /* ignore this exception for 19 for now; known bug */
        }

        /*
         * Put in the third row, the pin/name/childName field is the same
         * as the second row
         */
        jsonString = "{\"id\": 3, \"pin\": 123456, \"name\":\"test2\"," +
            "\"childId\": 3, \"childName\":\"cName2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null) // no options
            .setTableName(tableName);
        putResult = handle.put(putRequest);
        assertNotNull(putResult.getVersion());

        /*
         * Create index, test query by indexed field
         */
        final String createIndexStatement =
            "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName +
            " (childName)";

        tableRequest =
            new TableRequest().setStatement(createIndexStatement);
        tres  = handle.tableRequest(tableRequest);
        tres.waitForCompletion(handle, 60000, 1000);
        assertEquals(tres.getTableState(), TableResult.State.ACTIVE);

        QueryRequest queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName +
                         " WHERE childName= \"cName2\"");
        QueryResult qres = handle.query(queryRequest);
        List<MapValue> results = qres.getResults();
        assertEquals(results.size(), 2);
        assertEquals("cName2",
                     results.get(0).get("childName").asString().getValue());

        /*
         * DELETE the first row
         */
        key = new MapValue().put("id", 1).put("pin", "654321").
            put("childId", 1);
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delResult = handle.delete(delRequest);
        assertTrue(delResult.getSuccess());

        /*
         * MultiDelete where name is test2
         */
        key = new MapValue().put("pin", "123456");
        MultiDeleteRequest multiDelRequest = new MultiDeleteRequest()
            .setKey(key)
            .setTableName(tableName);

        MultiDeleteResult mRes = handle.multiDelete(multiDelRequest);
        assertEquals(mRes.getNumDeletions(), 2);

        /*
         * There should be no record in the table now
         */
        queryRequest = new QueryRequest().
            setStatement("SELECT * from " + tableName);
        qres = handle.query(queryRequest);
        results = qres.getResults();
        assertEquals(results.size(), 0);
    }

    /*
     * This test runs in both secure and not secure environments,
     * ensuring that the requests succeed. Avoid security-dependent
     * operations
     */
    @Test
    public void testSystem()
        throws Exception {

        SystemResult dres = doSysOp(handle, "create namespace myns");

        dres = doSysOp(handle, "show namespaces");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show users");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show as json user admin");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show as json roles");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        /*
         * Create a table using this mechanism.
         */
        dres = doSysOp(handle,
                       "create table foo(id integer, primary key(id))");

        dres = doSysOp(handle, "show as json tables");
        assertNotNull(dres.getResultString());
    }

    @Test
    public void testNamespaces()
        throws Exception {

        /*
         * Note: tables will be dropped by a cascading drop of the namespace
         */

        final String parentName = "myns:parent";
        final String childName = "myns:parent.child";
        final int numParent = 30;
        final int numChild = 40;

        doSysOp(handle, "create namespace myns");

        /* parent in myns */
        TableRequest treq = new TableRequest().setStatement(
            "create table myns:parent(id integer, primary key(id))");
        TableResult tres = handle.tableRequest(treq);
        tres.waitForCompletion(handle, 100000, 1000);

        /* child in myns */
        treq = new TableRequest().setStatement(
            "create table myns:parent.child(cid integer, name string, " +
            "primary key(cid))");
        tres = handle.tableRequest(treq);
        tres.waitForCompletion(handle, 100000, 1000);

        /* put data in both tables */
        PutRequest preq = new PutRequest();
        MapValue value = new MapValue();
        for (int i = 0; i < numParent; i++) {
            value.put("name", "myname"); // ignored in parent
            value.put("id", i);
            preq.setTableName(parentName).setValue(value);
            PutResult pres = handle.put(preq);
            assertNotNull("Parent put failed", pres.getVersion());
            for (int j = 0; j < numChild; j++) {
                value.put("cid", j); // ignored in parent
                preq.setTableName(childName).setValue(value);
                pres = handle.put(preq);
                assertNotNull("Child put failed", pres.getVersion());
                assertNoUnits(pres);
            }
        }

        /* get parent */
        GetRequest getReq = new GetRequest().setTableName(parentName)
            .setKey(new MapValue().put("id", 1));
        GetResult getRes = handle.get(getReq);
        assertNotNull(getRes.getValue());

        /* get child */
        getReq = new GetRequest().setTableName(childName)
            .setKey(new MapValue().put("id", 1).put("cid", 1));
        getRes = handle.get(getReq);
        assertNotNull(getRes.getValue());
        assertNoUnits(getRes);

        try {
            /* query parent */
            String query = "select * from " + parentName;
            List<MapValue> res = doQuery(handle, query);
            assertEquals(numParent, res.size());

            /* query child */
            query = "select * from " + childName;
            res = doQuery(handle, query);
            assertEquals(numParent * numChild, res.size());

            /* prepared query on child */
            res = doPreparedQuery(handle, query);
            assertEquals(numParent * numChild, res.size());
        } catch (RequestTimeoutException rte) {
            /*
            if (!(rte.getCause() instanceof SystemException)) {
                throw rte;
            }
            */
            /* ignore this exception for 19 for now; known bug */
        }

        /* test ListTables with namespace */
        ListTablesRequest listTables =
            new ListTablesRequest().setNamespace("myns");
        ListTablesResult lres = handle.listTables(listTables);
        assertEquals(2, lres.getTables().length);
    }

    /**
     * Test that the limit on # of operations in WriteMultiple isn't enforced
     */
    @Test
    public void writeMultipleTest() {

        doSysOp(handle, "create table foo(id1 string, " +
                "id2 integer, primary key(shard(id1), id2))");

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for (int i = 0; i < BATCH_OP_NUMBER_LIMIT +1; i++) {
            PutRequest pr = new PutRequest().setTableName("foo").
                setValueFromJson("{\"id1\":\"a\", \"id2\":" + i + "}", null);
            wmReq.add(pr, false);
        }
        try {
            WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
            assertEquals(BATCH_OP_NUMBER_LIMIT + 1, wmRes.size());
        } catch (BatchOperationNumberLimitException ex) {
            fail("operation should have succeeded");
        }
    }

    @Test
    public void testUnsupportedOps() throws Exception {

        doSysOp(handle, "create table foo(id integer, primary key(id))");
        try {
            TableUsageRequest req =
                new TableUsageRequest().setTableName("foo");
            handle.getTableUsage(req);
            fail("op should have failed");
        } catch (OperationNotSupportedException e) {
            // success
        }
    }

    @Test
    public void testLargeRow() {
        doLargeRow(handle, true);
    }

    /*
     * Ensure that MR table DDL statements pass through properly
     */
    @Test
    public void testMultiRegion() {
        final String show = "show regions";
        final String createRegion = "create region remoteRegion";
        final String setRegion = "set local region localRegion";
        final String createTable = "create table mrtable(id integer, " +
            "primary key(id)) in regions localRegion";

        /*
         * doSysOp will throw on any failures; there is no "error" return
         * information in SystemResult.
         */
        SystemResult res = doSysOp(handle, createRegion);
        res = doSysOp(handle, setRegion);
        res = doSysOp(handle, show);
        String resString = res.getResultString();
        assertTrue(resString.contains("localRegion") &&
                   resString.contains("remoteRegion"));

        /* count sys tables first */
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = handle.listTables(listTables);
        int numSysTables = lres.getTables().length;

        res = doSysOp(handle, createTable);

        lres = handle.listTables(listTables);
        assertEquals(numSysTables + 1, lres.getTables().length);

        /* this will throw if the table doesn't exist */
        getTable("mrtable", handle);
    }

    @Test
    public void testSecureSysOp()
        throws Exception {

        /*
         * This test requires a secure configuration
         */
        if (!secure) {
            return;
        }

        SystemResult dres = doSysOp(handle, "create namespace myns");

        /* create a user -- use a password with white space and quotes */
        dres = doSysOp(handle,
                       "create user newuser " +
                       "identified by 'ChrisToph \"_12&%'");

        dres = doSysOp(handle, "show namespaces");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show users");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show as json roles");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        dres = doSysOp(handle, "show as json user admin");
        assertNotNull(dres.getResultString());
        assertNull(dres.getOperationId());

        String[] roles = handle.listRoles();
        /*
         * The number of default roles may vary with the kv release.
         * Don't assume a specific number. This range is safe for now.
         */
        assertTrue(roles.length > 2 && roles.length < 10);
    }

    @Test
    public void testSystemExceptions()
        throws Exception {

        /* requires secure configuration */
        if (!secure) {
            return;
        }
        final String createTable = "Create table " +
            "foo(id integer, name string, primary key(id))";
        final String createIndex = "create index idx on foo(name)";

        doSysOp(handle, "create namespace myns");

        /* create table and index for later */
        doSysOp(handle, createTable);
        doSysOp(handle, createIndex);

        /* create a user -- use a password with white space and quotes */
        doSysOp(handle,
                "create user newuser " +
                "identified by 'ChrisToph \"_12&%'");
        /*
         * test error conditions
         */
        try {
            doSysOp(handle, "drop namespace not_a_namespace");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            // success
        }
        try {
            doSysOp(handle, "show as json user not_a_user");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            // success
        }
        try {
            doSysOp(handle, "show as json role not_a_role");
            fail("operation should have failed");
        } catch (ResourceNotFoundException e) {
            // success
        }

        try {
            doSysOp(handle,
                    "create user newuser " +
                    "identified by 'Chrioph \"_12&%'");
            fail("operation should have failed");
        } catch (ResourceExistsException e) {
            // success
        }

        try {
            doSysOp(handle, "create namespace myns");
            fail("operation should have failed");
        } catch (ResourceExistsException e) {
            // success
        }

        try {
            doSysOp(handle, "drop table not_a_table");
            fail("operation should have failed");
        } catch (TableNotFoundException e) {
            // success
        }

        try {
            doSysOp(handle, "drop index no_index on foo");
            fail("operation should have failed");
        } catch (IndexNotFoundException e) {
            // success
        }

        try {
            doSysOp(handle, createTable);
            fail("operation should have failed");
        } catch (TableExistsException e) {
            // success
        }

        try {
            doSysOp(handle, createIndex);
        } catch (IndexExistsException e) {
            // success
        }
    }

    protected static SystemResult doSysOp(NoSQLHandle nosqlHandle,
                                          String statement) {
        SystemResult dres =
            nosqlHandle.doSystemRequest(statement, 20000, 1000);
        assertTrue(dres.getOperationState() == SystemResult.State.COMPLETE);
        return dres;
    }

    static void assertNoUnits(Result res) {
        assertEquals(0, res.getReadKBInternal());
        assertEquals(0, res.getReadUnitsInternal());
        assertEquals(0, res.getWriteKBInternal());
        assertEquals(0, res.getWriteUnitsInternal());
    }
}
