/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;

import org.junit.Test;

public class RequestTest {

    @Test
    public void testQueryRequestCopyPreservesDurability() {
        QueryRequest request = new QueryRequest()
            .setDurability(Durability.COMMIT_SYNC);

        assertEquals(Durability.COMMIT_SYNC,
                     request.copyInternal().getDurability());
        assertEquals(Durability.COMMIT_SYNC,
                     request.copy().getDurability());
    }

    @Test
    public void testReplicaRequestsDoNotRetryWithoutIdempotencyToken() {
        RetryHandler handler = new DefaultRetryHandler(10, 0);
        RetryableException retryable = new SystemException("retryable");

        AddReplicaRequest addReplica = new AddReplicaRequest();
        assertFalse(addReplica.shouldRetry());
        assertFalse(handler.doRetry(addReplica, 0, retryable));

        DropReplicaRequest dropReplica = new DropReplicaRequest();
        assertFalse(dropReplica.shouldRetry());
        assertFalse(handler.doRetry(dropReplica, 0, retryable));
    }

    @Test
    public void testSystemRequestAndResultToStringRedactPasswords() {
        final String secret = "ChrisToph \"_12&%";
        final String statement =
            "CREATE USER u IDENTIFIED BY '" + secret + "'";
        final String redactedStatement =
            "CREATE USER u IDENTIFIED BY <redacted>";

        SystemRequest request =
            new SystemRequest().setStatement(statement.toCharArray());
        assertTrue(request.toString().contains(redactedStatement));
        assertFalse(request.toString().contains(secret));

        SystemResult result = new SystemResult()
            .setStatement(statement)
            .setResultString("completed: " + statement)
            .setState(SystemResult.State.COMPLETE);
        String resultString = result.toString();
        assertTrue(resultString.contains(redactedStatement));
        assertFalse(resultString.contains(secret));

        assertEquals(statement, result.getStatement());
        assertEquals("completed: " + statement, result.getResultString());
    }

    @Test
    public void testSystemToStringPreservesNonSensitiveStatements() {
        final String statement = "CREATE NAMESPACE mynamespace";

        SystemRequest request =
            new SystemRequest().setStatement(statement.toCharArray());
        assertTrue(request.toString().contains(statement));

        SystemResult result = new SystemResult()
            .setStatement(statement)
            .setResultString("created namespace")
            .setState(SystemResult.State.COMPLETE);
        assertTrue(result.toString().contains(statement));
        assertTrue(result.toString().contains("created namespace"));
    }

    @Test
    public void testPreparedStatementDoesNotExposeMutableProxyBytes() {
        ArrayList<byte[]> proxyStatements = new ArrayList<byte[]>();
        byte[] proxyStatement = new byte[] {1, 2, 3};
        proxyStatements.add(proxyStatement);
        ArrayList<String> namespaces = new ArrayList<String>();
        namespaces.add("ns");
        ArrayList<String> tableNames = new ArrayList<String>();
        tableNames.add("table");

        PreparedStatement prepared = new PreparedStatement(
            "select * from table",
            null,
            null,
            proxyStatements,
            null,
            0,
            0,
            null,
            namespaces,
            tableNames,
            (byte)5,
            0);

        proxyStatement[0] = 9;
        proxyStatements.set(0, new byte[] {9, 9, 9});
        assertEquals(1, prepared.getProxyStatement(0)[0]);

        byte[] returned = prepared.getProxyStatement(0);
        returned[0] = 8;
        assertEquals(1, prepared.getProxyStatement(0)[0]);

        PreparedStatement copy = prepared.copyStatement();
        returned = copy.getProxyStatement(0);
        returned[0] = 7;
        assertEquals(1, copy.getProxyStatement(0)[0]);
        assertEquals(1, prepared.getProxyStatement(0)[0]);
    }
}
