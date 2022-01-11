/**
 * Test base for tests that talk to a server. The server (proxy) may be
 * one of:
 *  o on-premise configuration, not secure
 *  o on-premise configuration, secure
 *  o cloud simulator configuration
 *
 * Information required to connect and run tests
 *  1. endpoint
 *  2. type of server for conditional tests
 *  3. secure on-premise adds authentication information:
 *    o user name and password of an authorized user
 *    o path to a trust store for SSL configuration
 *
 * Each test case creates and closes a new NoSQLHandle. Test cases can create
 * their own handles as needed for the tests.
 *
 * The assumption is that a server of the specified type has been started and
 * its endpoint has been communicated to the test framework.
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashSet;

import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.MapValue;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ProxyTestBase {
    protected static String VERBOSE = "test.verbose";
    protected static String ENDPOINT = "test.endpoint";
    protected static String SERVER_TYPE = "test.serverType";
    protected static String ONPREM = "test.onprem";
    protected static String SECURE = "test.secure";
    protected static String LOCAL = "test.local";
    protected static String USER = "test.user";
    protected static String TRUST_STORE = "test.trust";
    protected static String TRUST_STORE_PASSWORD = "test.trust.password";
    protected static String PASSWORD = "test.password";
    protected static String TRACE = "test.trace";
    protected static int DEFAULT_DDL_TIMEOUT = 15000;
    protected static int DEFAULT_DML_TIMEOUT = 5000;
    protected static String TEST_TABLE_NAME = "drivertest";

    protected static String serverType;
    protected static String endpoint;
    protected static boolean verbose;
    protected static boolean onprem;
    protected static boolean secure;
    protected static String user;       // only if onprem && secure
    protected static String password;   // only if onprem && secure
    protected static String trustStore; // only if onprem && secure
    protected static String trustStorePassword; // only if onprem && secure
    protected static boolean local;
    protected static URL serviceURL;
    /* is "string as uuid" supported? */
    protected static boolean uuidSupported;
    /* is casting an array to a record supported? */
    protected static boolean arrayAsRecordSupported;
    /* trace tests by printing start for each case */
    protected static boolean trace;

    /*
     * track existing tables and don't drop them
     */
    protected static HashSet<String> existingTables;

    protected NoSQLHandle handle;

    @Rule
    public final TestRule watchman = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (trace) {
                System.out.println(java.time.Instant.now() +
                                   " Starting test: " +
                                   description.getMethodName());
            }
        }
    };

    /**
     * Acquire system properties and ensure required properties are
     * present.
     */
    @BeforeClass
    public static void staticSetup() {
        endpoint = System.getProperty(ENDPOINT);
        serverType = System.getProperty(SERVER_TYPE);
        onprem = Boolean.getBoolean(ONPREM);
        if (serverType.equals("onprem")) {
            onprem = true;
        }
        secure = Boolean.getBoolean(SECURE);
        verbose = Boolean.getBoolean(VERBOSE);
        local = Boolean.getBoolean(LOCAL);
        trace = Boolean.getBoolean(TRACE);
        /* these features are not yet available in the cloud */
        uuidSupported = onprem;
        arrayAsRecordSupported = onprem;

        if (secure) {
            if (!onprem) {
                throw new IllegalArgumentException(
                    "Illegal test combination: secure requires onprem");
            }
            user = System.getProperty(USER);
            password = System.getProperty(PASSWORD);
            trustStore = System.getProperty(TRUST_STORE);
            trustStorePassword = System.getProperty(TRUST_STORE_PASSWORD);
            if (user == null || password == null || trustStore == null) {
                throw new IllegalArgumentException(
                    "Secure configuration requires user, password, and " +
                    " trustStore");
            }
            /* the trust store containing SSL cert for the proxy */
            System.setProperty("javax.net.ssl.trustStore", trustStore);
            if (trustStorePassword != null) {
                System.setProperty("javax.net.ssl.trustStorePassword",
                                   trustStorePassword);
            }
        }

        if (!local && (endpoint == null || serverType == null)) {
            throw new IllegalArgumentException(
                "Test requires " + ENDPOINT + " and " + SERVER_TYPE +
                " system properties");
        }

    }

    /*
     * In mvn the test dir is in the pom.xml file in the
     * config for the maven-surefire-plugin that runs junit
     * -- target/test-run
     */
    protected static String getTestDir() {
        return ".";
    }

    /*
     * Tests are run from driver/target/test-run, return the relative path
     * to the test resources directory
     */
    protected static String getResourcesDir() {
        return "../../src/test/resources/";
    }

    /**
     * run the statement, assumes success
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String statement,
                                                TableLimits limits) {
        return tableOperation(handle, statement, limits, DEFAULT_DDL_TIMEOUT);
    }

    /**
     * run the statement, assumes success, exception is thrown on error
     */
    protected static TableResult tableOperation(NoSQLHandle handle,
                                                String statement,
                                                TableLimits limits,
                                                int waitMillis) {
        assertTrue(waitMillis > 500);
        TableRequest tableRequest = new TableRequest()
            .setStatement(statement)
            .setTableLimits(limits)
            .setTimeout(DEFAULT_DDL_TIMEOUT);

        TableResult tres =
            handle.doTableRequest(tableRequest, waitMillis, 1000);
        return tres;
    }
    /**
     * run the statement, assumes success, exception is thrown on error
     */
    protected static TableResult alterTableLimits(NoSQLHandle handle,
                                                  String tableName,
                                                  TableLimits limits) {
        TableRequest tableRequest = new TableRequest()
            .setTableLimits(limits)
            .setTableName(tableName)
            .setTimeout(DEFAULT_DDL_TIMEOUT);

        return handle.doTableRequest(tableRequest, DEFAULT_DDL_TIMEOUT, 1000);
    }

    /**
     * acquire a handle to the service and clean up tables
     */
    @Before
    public void beforeTest() throws Exception {
        /*
         * Configure and get the handle
         */
        handle = getHandle(endpoint);

        /* track existing tables and don't drop them */
        existingTables = new HashSet<String>();
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = handle.listTables(listTables);
        for (String tableName: lres.getTables()) {
            existingTables.add(tableName);
        }
    }

    /**
     * clean up tables and close handle
     */
    @After
    public void afterTest() throws Exception {

        if (handle != null) {
            dropAllTables(handle, true);
            handle.close();
        }
    }

    protected static void dropAllTables(NoSQLHandle nosqlHandle,
                                        boolean wait) {

        /* get the names of all tables */
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = nosqlHandle.listTables(listTables);
        ArrayList<TableResult> droppedTables = new ArrayList<TableResult>();
        for (String tableName: lres.getTables()) {
            /* on-prem config may find system tables, which can't be dropped */
            if (tableName.startsWith("SYS$") ||
                existingTables.contains(tableName)) {
                continue;
            }
            boolean doWait = wait;

            /*
             * If this is a child table, wait for it to drop or dropping
             * its parent will fail
             */
            if (tableName.contains(".")) {
                doWait = true;
            }

            /* ignore, but note exceptions */
            try {
                if (doWait) {
                    dropTable(nosqlHandle, tableName);
                    continue;
                }
                TableResult tres = dropTableWithoutWait(nosqlHandle, tableName);
                droppedTables.add(tres);
            } catch (TableNotFoundException tnfe) {
                /* this is expected in 20.X and older */
                if (checkKVVersion(nosqlHandle, 21, 1, 1)) {
                    System.err.println("DropAllTables: drop fail, table "
                                       + tableName + ": " + tnfe);
                }
            } catch (Exception e) {
                System.err.println("DropAllTables: drop fail, table "
                                   + tableName + ": " + e);
                if (doWait) {
                    continue;
                }
            }
        }

        if (wait) {
            return;
        }

        /*
         * don't wait for ACTIVE state. This may mean occasional
         * failures but as long as tests pass that is ok.
         */

        /* wait for all tables dropped */
        for (TableResult tres: droppedTables) {
            /* ignore, but note exceptions */
            try {
                tres.waitForCompletion(nosqlHandle, 30000, 300);
            } catch (TableNotFoundException tnfe) {
                /* this is expected in 20.X and older */
                if (checkKVVersion(nosqlHandle, 21, 1, 1)) {
                    System.err.println("DropAllTables: drop wait fail, table "
                                       + tres + ": " + tnfe);
                }
            } catch (Exception e) {
                System.err.println("DropAllTables: drop wait fail, table "
                                   + tres + ": " + e);
            }
        }
    }

    protected void dropTable(String tableName) {
        dropTable(handle, tableName);
    }

    static void dropTable(NoSQLHandle nosqlHandle, String tableName) {
        try {
            TableResult tres = dropTableWithoutWait(nosqlHandle, tableName);

            if (tres.getTableState().equals(TableResult.State.DROPPED)) {
                return;
            }

            tres.waitForCompletion(nosqlHandle, 20000, 1000);
        } catch (TableNotFoundException e) {
            /* 20.2 and below have a known issue with drop table */
            if (checkKVVersion(nosqlHandle, 20, 3, 1) == true) {
                throw e;
            }
        }
    }

    static private TableResult dropTableWithoutWait(NoSQLHandle nosqlHandle,
                                                    String tableName) {
        final String dropTableDdl = "drop table if exists " + tableName;

        TableRequest tableRequest = new TableRequest()
            .setStatement(dropTableDdl)
            .setTimeout(100000);

        TableResult tres = nosqlHandle.tableRequest(tableRequest);
        assertNotNull(tres);
        return tres;
    }

    protected NoSQLHandle getHandle(String ep) {

        NoSQLHandleConfig config = new NoSQLHandleConfig(ep);
        serviceURL = config.getServiceURL();
        return setupHandle(config);
    }

    /* Set configuration values for the handle */
    protected NoSQLHandle setupHandle(NoSQLHandleConfig config) {

        /*
         * 5 retries, default retry algorithm
         */
        config.configureDefaultRetryHandler(5, 0);

        config.setConnectionPoolSize(3);
        config.setNumThreads(3);
        config.setRequestTimeout(30000);

        configAuth(config);

        /* allow test cases to add/modify handle config */
        perTestHandleConfig(config);

        NoSQLHandle h = getHandle(config);

        /* this will set up the right protocol serial version */
        /* and get the proxy/kv versions into the client */
        try {
            getTable("noop", h);
        } catch (Exception e) {
            /* ignore errors */
        }

        return h;
    }

    /**
     * sub classes can override this to affect the handle config
     */
    protected void perTestHandleConfig(NoSQLHandleConfig config) {
        /* no-op */
    }

    /**
     * get a handle based on the config
     */
    protected NoSQLHandle getHandle(NoSQLHandleConfig config) {
        /*
         * Create a Logger, set to WARNING by default.
         */
        Logger logger = Logger.getLogger(getClass().getName());
        String level = System.getProperty("test.loglevel");
        if (level == null) {
            level = "WARNING";
        }
        logger.setLevel(Level.parse(level));
        config.setLogger(logger);

        /*
         * Open the handle
         */
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }

    void assertReadKB(Result res) {
        if (onprem) {
            return;
        }
        assertTrue(res.getReadKBInternal() > 0);
    }

    void assertWriteKB(Result res) {
        if (onprem) {
            return;
        }
        assertTrue(res.getWriteKBInternal() > 0);
    }

    protected String getServiceHost() {
        return serviceURL.getHost();
    }

    protected int getServicePort() {
        return serviceURL.getPort();
    }

    /**
     * At this time these unit tests will not talk to the cloud service
     */
    protected void configAuth(NoSQLHandleConfig config) {
        if (onprem) {
            if (secure) {
                config.setAuthorizationProvider(
                    new StoreAccessTokenProvider(user, password.toCharArray()));
            } else {
                config.setAuthorizationProvider(new StoreAccessTokenProvider());
            }
        } else {
            /* cloud simulator */
            config.setAuthorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorizationString(Request request) {
                        return "Bearer cloudsim";
                    }

                    @Override
                    public void close() {
                    }
            });
        }
    }

    protected static byte[] genBytes(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    protected static String genString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    /**
     * This is factored here so that it can be used by both the cloudsim-based
     * tests and kv.
     */
    protected static void doLargeRow(NoSQLHandle thandle,
                                     boolean doWriteMultiple) {
        final String createTableStatement =
            "create table bigtable(" +
            "id integer, " +
            "large array(string), " +
            "primary key(id))";
        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        thandle.doTableRequest(tableRequest, DEFAULT_DDL_TIMEOUT, 1000);
        MapValue value = new MapValue().put("id", 1);
        ArrayValue array = createLargeStringArray(3500000);
        value.put("large", array);
        PutRequest preq = new PutRequest().setTableName("bigtable").
            setValue(value);
        PutResult pres = thandle.put(preq);
        assertNotNull(pres.getVersion());

        if (doWriteMultiple) {
            /*
             * Now with write multiple
             */
            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            /* don't reuse the PutRequest above, it has been modified */
            preq = new PutRequest().setTableName("bigtable").setValue(value);
            wmReq.add(preq, false);
            WriteMultipleResult wmRes = thandle.writeMultiple(wmReq);
            assertEquals(1, wmRes.size());
        }
    }

    static private ArrayValue createLargeStringArray(int size) {
        ArrayValue array = new ArrayValue();
        int tsize = 0;
        final String s = "abcdefghijklmnop";
        while (tsize < size) {
            array.add(s);
            tsize += s.length();
        }
        return array;
    }

    protected static List<MapValue> doQuery(NoSQLHandle qHandle, String query) {
        List<MapValue> results = new ArrayList<MapValue>();
        QueryRequest queryRequest = new QueryRequest().setStatement(query);
        do {
            QueryResult qres = qHandle.query(queryRequest);
            results.addAll(qres.getResults());
        } while (!queryRequest.isDone());
        return results;
    }

    protected static List<MapValue> doPreparedQuery(
        NoSQLHandle qHandle, String query) {

        List<MapValue> results = new ArrayList<MapValue>();
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(query);
        PrepareResult prepRet = qHandle.prepare(prepReq);
        assertNotNull(prepRet.getPreparedStatement());

        QueryRequest queryRequest =
            new QueryRequest().setPreparedStatement(prepRet);
        do {
            QueryResult qres = qHandle.query(queryRequest);
            results.addAll(qres.getResults());
        } while (!queryRequest.isDone());
        return results;
    }

    protected static TableResult getTable(String tableName,
                                          NoSQLHandle handle) {
        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName);
        return handle.getTable(getTable);
    }

    protected static void verbose(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    /*
     * convert a version string in X.Y.Z format to an
     * integer value of (X * 1M) + (Y * 1K) + Z
     * return -1 if the string isn't in valid X.Y.Z format
     */
    protected static int getIntegerVersion(String version) {
        if (version == null || version.length() < 5) {
            return -1;
        }
        String[] arr = version.split("\\.");
        if (arr == null || arr.length != 3) {
            return -1;
        }
        try {
            return (Integer.parseInt(arr[0]) * 1000000) +
                   (Integer.parseInt(arr[1]) * 1000) +
                   Integer.parseInt(arr[2]);
        } catch (Exception e) {}
        return -1;
    }

    protected static String getKVServerVersion(NoSQLHandle handle) {
        /*
         * Use the value returned from the proxy. If that doesn't
         * exist, fall back to an environment setting.
         * Note: currently, the proxy will not return this value. So the
         * environment setting will always be used.
         */
        String kvver =
                   ((NoSQLHandleImpl)handle).getClient().getKVServerVersion();
        if (getIntegerVersion(kvver) > 0) {
            return kvver;
        }
        return System.getenv("KV_SERVER_VERSION");
    }

    protected static String getKVClientVersion(NoSQLHandle handle) {
        /*
         * Use the value returned from the proxy. If that doesn't
         * exist, fall back to an environment setting.
         */
        String kvver =
                   ((NoSQLHandleImpl)handle).getClient().getKVClientVersion();
        if (getIntegerVersion(kvver) > 0) {
            return kvver;
        }
        return System.getenv("KV_CLIENT_VERSION");
    }

    protected static String getProxyVersion(NoSQLHandle handle) {
        /*
         * Use the value returned from the proxy. If that doesn't
         * exist, fall back to an environment setting.
         */
        String proxyver =
                   ((NoSQLHandleImpl)handle).getClient().getProxyVersion();
        if (getIntegerVersion(proxyver) > 0) {
            return proxyver;
        }
        return System.getenv("PROXY_VERSION");
    }

    private static String getMinimumKVVersion(NoSQLHandle handle) {
        /*
         * We need to use the minumum of the kv client and server versions to
         * determine what features should be valid to test.
         */
        String serverVer = getKVServerVersion(handle);
        String clientVer = getKVClientVersion(handle);
        int serverIntVer = getIntegerVersion(serverVer);
        if (serverIntVer < 0) {
            return clientVer;
        }
        int clientIntVer = getIntegerVersion(clientVer);
        if (clientIntVer < 0 || clientIntVer > serverIntVer) {
            return serverVer;
        }
        return clientVer;
    }

    /*
     * Used to skip test if run against KV prior to the specified version
     * <major>.<minor>.<patch>.
     */
    protected static void assumeKVVersion(NoSQLHandle handle,
                                          String test,
                                          int major,
                                          int minor,
                                          int patch) {
        if (checkKVVersion(handle, major, minor, patch)) {
            return;
        }
        assumeTrue("Skipping " + test + " if run against KV prior to " +
                   (major + "." + minor + "." + patch) + ": " +
                   getMinimumKVVersion(handle), false);
    }

    /*
     * Returns true if the current KV is >= version <major.minor.patch>
     */
    public static boolean checkKVVersion(NoSQLHandle handle,
                                         int major,
                                         int minor,
                                         int patch) {
        String minVersion = getMinimumKVVersion(handle);
        int minIntVersion = getIntegerVersion(minVersion);
        if (minIntVersion <= 0) {
            return false; // we have no way of knowing for sure
        }
        int desiredIntVersion = (major * 1000000) + (minor * 1000) + patch;
        return (minIntVersion >= desiredIntVersion);
    }
}
