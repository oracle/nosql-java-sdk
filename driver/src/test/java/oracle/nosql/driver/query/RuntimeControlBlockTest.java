/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import io.netty.handler.ssl.SslContext;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.PrepareQueryException;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;

import org.junit.Test;

/**
 * Tests topology selection and snapshot behavior during advanced query
 * execution.
 */
public class RuntimeControlBlockTest {

    private static String STORE1 = "store1";
    private static TopologyInfo STORE1_TOPO =
        new TopologyInfo(STORE1, 10, new int[] { 1, 2 });
    private static TopologyInfo STORE1_NEW_TOPO =
        new TopologyInfo("store1", 99, new int[]{ 5, 6 });

    private static String STORE2 = "store2";
    private static TopologyInfo STORE2_TOPO =
        new TopologyInfo(STORE2, 20, new int[] { 1, 2, 3, 4});

    private static TopologyInfo LEGACY_TOPO =
        new TopologyInfo(11, new int[] { 1, 2 });

    /**
     * Verifies that each UNION branch uses the topology frozen at execution
     * initialization, even after the client cache is refreshed.
     */
    @Test
    public void testBaseTopologyFrozenPerBranch() {

        TestClient client = new TestClient();
        try {
            client.putStoreTopology(STORE1_TOPO);
            client.putStoreTopology(STORE2_TOPO);

            PreparedStatement pstmt =
                createPreparedStatement(new String[] {STORE1, STORE2});
            RuntimeControlBlock rcb = createRuntimeControlBlock(client, pstmt);

            assertEquals(STORE1_TOPO, rcb.getBaseTopo());

            client.putStoreTopology(STORE1_NEW_TOPO);
            assertEquals(STORE1_TOPO, rcb.getBaseTopo());

            rcb.setUnionBranch(1);
            assertEquals(STORE2_TOPO, rcb.getBaseTopo());
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that a per-store prepared query must be prepared again when its
     * store topology is missing from the client cache.
     */
    @Test
    public void testMissingBranchTopologyRequiresReprepare() {

        TestClient client = new TestClient();
        try {
            PreparedStatement pstmt =
                createPreparedStatement(new String[]{ STORE1 });
            assertThrows(PrepareQueryException.class,
                         () -> createRuntimeControlBlock(client, pstmt));
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that legacy prepared queries require and use the single legacy
     * topology for every UNION branch.
     */
    @Test
    public void testLegacyPreparedStatementUsesLegacyTopology() {

        TestClient client = new TestClient();
        try {
            PreparedStatement pstmt = createPreparedStatement(null);
            assertThrows(PrepareQueryException.class,
                         () -> createRuntimeControlBlock(client, pstmt));

            client.setLegacyTopology(LEGACY_TOPO);

            RuntimeControlBlock rcb = createRuntimeControlBlock(client, pstmt);
            assertEquals(LEGACY_TOPO, rcb.getBaseTopo());

            rcb.setUnionBranch(1);
            assertEquals(LEGACY_TOPO, rcb.getBaseTopo());
        } finally {
            client.shutdown();
        }
    }

    /**
     * Verifies that internal query requests use sequence numbers matching the
     * base topology snapshot, rather than a later client-cache refresh.
     */
    @Test
    public void testInternalRequestUsesBaseTopologySnapshot() {
        TestClient client = new TestClient();
        try {
            client.putStoreTopology(STORE1_TOPO);

            PreparedStatement pstmt =
                createPreparedStatement(new String[] { STORE1 });
            QueryRequest request = new QueryRequest()
                .setPreparedStatement(pstmt);

            request.setTopoSeqNum(5);
            request.setStoreTopoSeqNums(Collections.singletonMap(STORE1, 5));

            QueryDriver driver = new QueryDriver(request);
            driver.setClient(client);
            RuntimeControlBlock rcb =
                new RuntimeControlBlock(driver, null, 1, 1, null);

            client.putStoreTopology(STORE1_NEW_TOPO);

            assertEquals(STORE1_TOPO, rcb.getBaseTopo());
            assertEquals(-1, request.topoSeqNum());
            assertEquals(Integer.valueOf(STORE1_TOPO.getSeqNum()),
                         request.getStoreTopoSeqNums().get(STORE1));
            assertEquals(request.getStoreTopoSeqNums(),
                         request.copyInternal().getStoreTopoSeqNums());
        } finally {
            client.shutdown();
        }
    }

    /**
     * Creates a runtime control block for the supplied prepared statement and
     * test client.
     */
    private RuntimeControlBlock createRuntimeControlBlock(
        TestClient client,
        PreparedStatement psmt) {

        QueryRequest request = new QueryRequest()
            .setPreparedStatement(psmt);
        QueryDriver driver = new QueryDriver(request);
        driver.setClient(client);
        return new RuntimeControlBlock(driver, null, 1, 1, null);
    }

    /**
     * Creates a multi-branch prepared statement with optional per-store branch
     * metadata. A null store list creates a legacy prepared statement.
     */
    private PreparedStatement createPreparedStatement(String[] storeNames) {

        int numBranches = (storeNames != null ? storeNames.length : 2);
        ArrayList<byte[]> proxyStatements = new ArrayList<byte[]>();
        ArrayList<String> namespaces = new ArrayList<String>();
        ArrayList<String> tableNames = new ArrayList<String>();
        ArrayList<String> branchStoreNames =
            (storeNames != null ? new ArrayList<String>() : null);

        for (int i = 0; i < numBranches; ++i) {
            proxyStatements.add(new byte[] { (byte)(i + 1) });
            namespaces.add("ns" + i);
            tableNames.add("table" + i);
            if (branchStoreNames != null) {
                branchStoreNames.add(storeNames[i]);
            }
        }

        return new PreparedStatement("select * from table",
                                     null,
                                     null,
                                     proxyStatements,
                                     null,
                                     0,
                                     1,
                                     null,
                                     namespaces,
                                     tableNames,
                                     (byte)5,
                                     0,
                                     branchStoreNames);
    }

    /**
     * Creates the minimal handle configuration needed by the test client.
     */
    private static NoSQLHandleConfig config() {
        AuthorizationProvider provider = new AuthorizationProvider() {
            /**
             * Returns a stable authorization value for local test requests.
             */
            @Override
            public String getAuthorizationString(Request request) {
                return "test";
            }

            /**
             * Performs no cleanup because the test provider owns no resources.
             */
            @Override
            public void close() {
            }
        };
        return new NoSQLHandleConfig("http://localhost:8080", provider);
    }

    private static class TestClient extends Client {

        private final Map<String, TopologyInfo> storeTopologies =
            new HashMap<String, TopologyInfo>();

        private TopologyInfo legacyTopology;

        /**
         * Creates an isolated client backed by the local test configuration.
         */
        TestClient() {
            super(null, config());
        }

        /**
         * Adds or replaces the cached topology for a store.
         */
        void putStoreTopology(TopologyInfo topology) {
            storeTopologies.put(topology.getStoreName(), topology);
        }

        /**
         * Sets the topology returned for legacy prepared queries.
         */
        void setLegacyTopology(TopologyInfo topology) {
            legacyTopology = topology;
        }

        /**
         * Returns an immutable snapshot of the per-store topology cache.
         */
        @Override
        public Map<String, TopologyInfo> getStoreTopoSnapshot() {
            return Collections.unmodifiableMap(
                new HashMap<String, TopologyInfo>(storeTopologies));
        }

        /**
         * Returns the topology used by legacy prepared queries.
         */
        @Override
        public TopologyInfo getTopology() {
            return legacyTopology;
        }

        /**
         * Creates the minimal HTTP client required by the test client without
         * issuing network requests.
         */
        @Override
        protected HttpClient createHttpClient(
            URL url,
            NoSQLHandleConfig httpConfig,
            SslContext sslCtx,
            Logger logger) {

            return new HttpClient("localhost",
                                  8080,
                                  1,
                                  0,
                                  0,
                                  0,
                                  0,
                                  null,
                                  0,
                                  "test",
                                  null);
        }
    }
}
