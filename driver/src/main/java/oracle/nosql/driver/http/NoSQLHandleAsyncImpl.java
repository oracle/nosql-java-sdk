/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.UserInfo;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
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
import oracle.nosql.driver.ops.QueryPublisher;
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
import oracle.nosql.driver.util.LogUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

import javax.net.ssl.SSLException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class NoSQLHandleAsyncImpl implements NoSQLHandleAsync {
    private static final int cores = Runtime.getRuntime().availableProcessors();

    /*
     * The HTTP client. This is not final so that it can be nulled upon
     * close.
     */
    private final Client client;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /* thread-pool for scheduling tasks */
    private final ScheduledExecutorService taskExecutor;

    public NoSQLHandleAsyncImpl(NoSQLHandleConfig config) {
        configNettyLogging();
        final Logger logger = getLogger(config);
        /*
         * config SslContext first, on-prem authorization provider
         * will reuse the context in NoSQLHandleConfig
         */
        configSslContext(config);
        taskExecutor = new ScheduledThreadPoolExecutor(cores /* core threads */,
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    final Thread t = Executors.defaultThreadFactory()
                                              .newThread(r);
                    t.setName(String.format("nosql-task-executor-%s",
                                            threadNumber.getAndIncrement()));
                    t.setDaemon(true);
                    t.setUncaughtExceptionHandler((thread, error) -> {
                        if (ConcurrentUtil.unwrapCompletionException(error)
                            instanceof RejectedExecutionException) {
                            /*
                             * Ignore uncaught error for rejected exception
                             * since that is expected to happen during
                             * executor shut down.
                             */
                            return;
                        }
                        logger.warning(() -> String.format(
                            "Uncaught exception from %s: %s",
                            error, LogUtil.getStackTrace(error)));
                    });
                    return t;
                }
            });
        client = new Client(logger, config, taskExecutor);
        try {
            /* configAuthProvider may use client */
            configAuthProvider(logger, config);
        } catch (RuntimeException re) {
            /* cleanup client */
            client.shutdown();
            taskExecutor.shutdown();
            throw re;
        }
    }

    /**
     * Returns the logger used for the driver. If no logger is specified
     * create one based on this class name.
     */
    private Logger getLogger(NoSQLHandleConfig config) {
        if (config.getLogger() != null) {
            return config.getLogger();
        }

        /*
         * The default logger logs at INFO. If this is too verbose users
         * must create a logger and pass it in.
         */
        Logger logger = Logger.getLogger(getClass().getName());
        return logger;
    }

    /**
     * Configures the logging of Netty library.
     */
    private void configNettyLogging() {
        /*
         * Configure default Netty logging using Jdk Logger.
         */
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
    }

    private void configSslContext(NoSQLHandleConfig config) {
        if (config.getSslContext() != null) {
            return;
        }
        if (config.getServiceURL().getProtocol().equalsIgnoreCase("HTTPS")) {
            try {
                SslContextBuilder builder = SslContextBuilder.forClient();
                if (config.getSSLCipherSuites() != null) {
                    builder.ciphers(config.getSSLCipherSuites());
                }
                if (config.getSSLProtocols() != null) {
                    builder.protocols(config.getSSLProtocols());
                }
                builder.sessionTimeout(config.getSSLSessionTimeout());
                builder.sessionCacheSize(config.getSSLSessionCacheSize());
                config.setSslContext(builder.build());
            } catch (SSLException se) {
                throw new IllegalStateException(
                        "Unable to start handle with SSL", se);
            }
        }
    }

    private void configAuthProvider(Logger logger, NoSQLHandleConfig config) {
        final AuthorizationProvider ap = config.getAuthorizationProvider();
        if (ap instanceof StoreAccessTokenProvider) {
            final StoreAccessTokenProvider stProvider =
                    (StoreAccessTokenProvider) ap;
            if (stProvider.getLogger() == null) {
                stProvider.setLogger(logger);
            }
            if (stProvider.isSecure() &&
                    stProvider.getEndpoint() == null) {
                String endpoint = config.getServiceURL().toString();
                if (endpoint.endsWith("/")) {
                    endpoint = endpoint.substring(0, endpoint.length() - 1);
                }
                stProvider.setEndpoint(endpoint)
                        .setSslContext(config.getSslContext())
                        .setSslHandshakeTimeout(
                                config.getSSLHandshakeTimeout());
            }

        } else if (ap instanceof SignatureProvider) {
            SignatureProvider sigProvider = (SignatureProvider) ap;
            if (sigProvider.getLogger() == null) {
                sigProvider.setLogger(logger);
            }
            sigProvider.prepare(config);
            if (config.getAuthRefresh()) {
                sigProvider.setOnSignatureRefresh(new SigRefresh());
                client.createAuthRefreshList();
            }
        }
    }

    @Override
    public CompletableFuture<DeleteResult> delete(DeleteRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<GetResult> get(GetRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<PutResult> put(PutRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<WriteMultipleResult> writeMultiple(
        WriteMultipleRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<MultiDeleteResult> multiDelete(
        MultiDeleteRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<QueryResult> query(QueryRequest request) {
        return queryAsync(request);
    }

    CompletableFuture<QueryResult> queryAsync(QueryRequest request) {
        return executeASync(request)
            .thenCompose(result -> {
                /* Complex queries need RCB, run asynchronously */
                if (!request.isSimpleQuery()) {
                    // TODO supplyAsync runs in fork-join pool.
                    //  Change to dedicated pool
                    return CompletableFuture.supplyAsync(() -> result);
                }
                return CompletableFuture.completedFuture(result);
            })
            .thenApply(result -> ((QueryResult) result));
    }

    @Override
    public Flow.Publisher<MapValue> queryPaginator(QueryRequest request) {
        return new QueryPublisher(this, request);
    }

    @Override
    public CompletableFuture<PrepareResult> prepare(PrepareRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<TableResult> tableRequest(TableRequest request) {
       return executeASync(request).thenApply(tres -> {
           TableResult res = (TableResult) tres;
           /* update rate limiters, if table has limits */
           client.updateRateLimiters(res.getTableName(), res.getTableLimits());
           return res;
       });
    }

    @Override
    public CompletableFuture<TableResult> getTable(GetTableRequest request) {
        return executeASync(request).thenApply(tres -> {
            TableResult res = (TableResult) tres;
            /* update rate limiters, if table has limits */
            client.updateRateLimiters(res.getTableName(), res.getTableLimits());
            return res;
        });
    }

    @Override
    public CompletableFuture<SystemResult> systemRequest(
        SystemRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<SystemResult> systemStatus(
        SystemStatusRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<TableUsageResult> getTableUsage(
        TableUsageRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<ListTablesResult> listTables(
        ListTablesRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<GetIndexesResult> getIndexes(
        GetIndexesRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<TableResult> addReplica(
        AddReplicaRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<TableResult> dropReplica(
        DropReplicaRequest request) {
        return executeASync(request);
    }

    @Override
    public CompletableFuture<ReplicaStatsResult> getReplicaStats(
        ReplicaStatsRequest request) {
        return executeASync(request);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            client.shutdown();
            taskExecutor.shutdown();
        }
    }

    @Override
    public CompletableFuture<String[]> listNamespaces() {
        return doSystemRequest("show as json namespaces")
            .thenApply((SystemResult dres )-> {
                String jsonResult = dres.getResultString();
                if (jsonResult == null) {
                    return null;
                }
                MapValue root = JsonUtils.createValueFromJson(jsonResult, null)
                    .asMap();

                FieldValue namespaces = root.get("namespaces");
                if (namespaces == null) {
                    return null;
                }

                ArrayList<String> results = new ArrayList<String>(
                    namespaces.asArray().size());
                for (FieldValue val : namespaces.asArray()) {
                    results.add(val.getString());
                }
                return results.toArray(new String[0]);
            });
    }

    @Override
    public CompletableFuture<UserInfo[]> listUsers() {
        return doSystemRequest("show as json users")
            .thenApply((SystemResult dres) -> {
                String jsonResult = dres.getResultString();
                if (jsonResult == null) {
                    return null;
                }

                MapValue root = JsonUtils.createValueFromJson(
                    jsonResult, null).asMap();

                FieldValue users = root.get("users");
                if (users == null) {
                    return null;
                }

                ArrayList<UserInfo> results = new ArrayList<UserInfo>(
                    users.asArray().size());

                for (FieldValue val : users.asArray()) {
                    String id = val.asMap().getString("id");
                    String name = val.asMap().getString("name");
                    results.add(new UserInfo(id, name));
                }
                return results.toArray(new UserInfo[0]);
            });
    }

    @Override
    public CompletableFuture<String[]> listRoles() {
        return doSystemRequest("show as json roles")
            .thenApply((SystemResult dres) -> {
                String jsonResult = dres.getResultString();
                if (jsonResult == null) {
                    return null;
                }
                MapValue root = JsonUtils.createValueFromJson(
                    jsonResult, null).asMap();

                FieldValue roles = root.get("roles");
                if (roles == null) {
                    return null;
                }

                ArrayList<String> results = new ArrayList<String>(
                    roles.asArray().size());
                for (FieldValue val : roles.asArray()) {
                    String role = val.asMap().getString("name");
                    results.add(role);
                }
                return results.toArray(new String[0]);
            });
    }

    /**
     * Internal method used by list* methods that defaults timeouts.
     */
    private CompletableFuture<SystemResult> doSystemRequest(String statement) {
        return doSystemRequest(statement, 30000, 1000);
    }

    @Override
    public CompletableFuture<TableResult> doTableRequest(TableRequest request,
                                                         int timeoutMs,
                                                         int pollIntervalMs) {

        return tableRequest(request).thenCompose((TableResult res) ->
            res.waitForCompletionAsync(this, timeoutMs, pollIntervalMs)
            .thenApply(v -> res));
    }

    @Override
    public CompletableFuture<SystemResult> doSystemRequest(String statement,
                                                           int timeoutMs,
                                                           int pollIntervalMs) {
        checkClient();
        SystemRequest dreq =
            new SystemRequest().setStatement(statement.toCharArray());
        return systemRequest(dreq).thenCompose((SystemResult dres) ->
            dres.waitForCompletionAsync(this, timeoutMs, pollIntervalMs)
            .thenApply(v -> dres));
    }

    @Override
    public StatsControl getStatsControl() {
        return client.getStatsControl();
    }

    void checkClient() {
        if (isClosed.get()) {
            throw new IllegalStateException("NoSQLHandle has been closed");
        }
    }

    /**
     * @hidden
     * For testing use
     */
    public Client getClient() {
        return client;
    }

    /**
     * @hidden
     * For testing use
     */
    public short getSerialVersion() {
        return client.getSerialVersion();
    }

    /**
     * @hidden
     *
     * Testing use only.
     */
    public void setDefaultNamespace(String ns) {
        client.setDefaultNamespace(ns);
    }

    @SuppressWarnings("unchecked")
    <T extends Result> CompletableFuture<T> executeASync(Request request) {
        checkClient();
        return client.execute(request).thenApply(result -> (T) result);
    }

    public ScheduledExecutorService getTaskExecutor() {
        return taskExecutor;
    }

    /**
     * Cloud service only.
     * The refresh method of this class is called when a Signature is refreshed
     * in SignatureProvider. This happens every 4 minutes or so. This mechanism
     * allows the authentication and authorization information cached by the
     * server to be refreshed out of band with the normal request path.
     */
    private class SigRefresh implements SignatureProvider.OnSignatureRefresh {

        /*
         * Attempt to refresh the server's authentication and authorization
         * information for a new signature.
         */
        @Override
        public void refresh(long refreshMs) {
            client.doRefresh(refreshMs);
        }
    }
}
