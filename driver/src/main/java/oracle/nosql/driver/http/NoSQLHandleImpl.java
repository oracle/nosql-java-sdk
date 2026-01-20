/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.UserInfo;
import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.ReplicaStatsRequest;
import oracle.nosql.driver.ops.ReplicaStatsResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.SystemStatusRequest;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.util.ConcurrentUtil;

/**
 * The methods in this class require non-null arguments. Because they all
 * ultimately call the Client class the check for null is done there in a
 * single place.
 */
public class NoSQLHandleImpl implements NoSQLHandle {
    /*
     * The HTTP client. This is not final so that it can be nulled upon
     * close.
     */
    private final NoSQLHandleAsyncImpl asyncHandle;

    public NoSQLHandleImpl(NoSQLHandleConfig config) {
        asyncHandle = new NoSQLHandleAsyncImpl(config);
    }

    @Override
    public DeleteResult delete(DeleteRequest request) {
        return executeSync(request);
    }

    @Override
    public GetResult get(GetRequest request) {
        return executeSync(request);
    }

    @Override
    public PutResult put(PutRequest request) {
        return executeSync(request);
    }

    @Override
    public WriteMultipleResult writeMultiple(WriteMultipleRequest request) {
        return executeSync(request);
    }

    @Override
    public MultiDeleteResult multiDelete(MultiDeleteRequest request) {
        return executeSync(request);
    }

    @Override
    public QueryResult query(QueryRequest request) {
        return ConcurrentUtil.awaitFuture(asyncHandle.query(request));
    }

    @Override
    public QueryIterableResult queryIterable(QueryRequest request) {
        asyncHandle.checkClient();
        return new QueryIterableResult(request, this);
    }

    @Override
    public PrepareResult prepare(PrepareRequest request) {
        return executeSync(request);
    }

    @Override
    public TableResult tableRequest(TableRequest request) {
        return executeSync(request);
    }

    @Override
    public TableResult getTable(GetTableRequest request) {
        return executeSync(request);
    }

    @Override
    public SystemResult systemRequest(SystemRequest request) {
        return executeSync(request);
    }

    @Override
    public SystemResult systemStatus(SystemStatusRequest request) {
        return executeSync(request);
    }

    @Override
    public TableUsageResult getTableUsage(TableUsageRequest request) {
        return executeSync(request);
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest request) {
        return executeSync(request);
    }

    @Override
    public GetIndexesResult getIndexes(GetIndexesRequest request) {
        return executeSync(request);
    }

    @Override
    public TableResult addReplica(AddReplicaRequest request) {
        return executeSync(request);
    }

    @Override
    public TableResult dropReplica(DropReplicaRequest request) {
        return executeSync(request);
    }

    @Override
    public ReplicaStatsResult getReplicaStats(ReplicaStatsRequest request) {
        return executeSync(request);
    }

    @Override
    public void close() {
        asyncHandle.close();
    }

    /**
     * Returns the namespaces in a store as an array of String.
     *
     * @return the namespaces, or null if none are found
     */
    @Override
    public String[] listNamespaces() {
        return ConcurrentUtil.awaitFuture(asyncHandle.listNamespaces());
    }

    /**
     * Returns the users in a store as an array of {@link UserInfo}.
     *
     * @return the list of users or null if none are found
     */
    @Override
    public UserInfo[] listUsers() {
        return ConcurrentUtil.awaitFuture(asyncHandle.listUsers());
    }

    /**
     * Returns the roles in a store as an array of String.
     *
     * @return the roles or null if none are found
     */
    @Override
    public String[] listRoles() {
        return ConcurrentUtil.awaitFuture(asyncHandle.listRoles());
    }


    /**
     * Internal method used by list* methods that defaults timeouts.
     */
    private SystemResult doSystemRequest(String statement) {
        return doSystemRequest(statement, 30000, 1000);
    }

    @Override
    public TableResult doTableRequest(TableRequest request,
                                      int timeoutMs,
                                      int pollIntervalMs) {
        TableResult res = tableRequest(request);
        res.waitForCompletion(this, timeoutMs, pollIntervalMs);
        return res;
    }

    @Override
    public SystemResult doSystemRequest(String statement,
                                        int timeoutMs,
                                        int pollIntervalMs) {
        checkClient();
        SystemRequest dreq =
            new SystemRequest().setStatement(statement.toCharArray());
        SystemResult dres = systemRequest(dreq);
        dres.waitForCompletion(this, timeoutMs, pollIntervalMs);
        return dres;
    }

    @Override
    public StatsControl getStatsControl() {
        return asyncHandle.getStatsControl();
    }

    /**
     * Ensure that the client exists and hasn't been closed;
     */
    private void checkClient() {
        asyncHandle.checkClient();
    }

    /**
     * @hidden
     * For testing use
     */
    public Client getClient() {
        return asyncHandle.getClient();
    }

    /**
     * @hidden
     * For testing use
     */
    public short getSerialVersion() {
        return asyncHandle.getSerialVersion();
    }

    /**
     * @hidden
     *
     * Testing use only.
     */
    public void setDefaultNamespace(String ns) {
        asyncHandle.setDefaultNamespace(ns);
    }

    @SuppressWarnings("unchecked")
    private <T extends Result> T executeSync(Request request) {
        return (T) ConcurrentUtil.awaitFuture(asyncHandle.executeASync(request));
    }
}
