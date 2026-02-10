/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import java.util.ArrayList;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.OperationNotSupportedException;
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
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.ReplicaStatsRequest;
import oracle.nosql.driver.ops.ReplicaStatsResult;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.SystemStatusRequest;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

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
    private Client client;

    public NoSQLHandleImpl(NoSQLHandleConfig config) {

        configNettyLogging();
        final Logger logger = getLogger(config);

        /*
         * config SslContext first, on-prem authorization provider
         * will reuse the context in NoSQLHandleConfig
         */
        configSslContext(config);
        client = new Client(logger, config);
        try {
            /* configAuthProvider may use client */
            configAuthProvider(logger, config);
        } catch (RuntimeException re) {
            /* cleanup client */
            client.shutdown();
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
    public DeleteResult delete(DeleteRequest request) {
        checkClient();
        return (DeleteResult) client.execute(request);
    }

    @Override
    public GetResult get(GetRequest request) {
        checkClient();
        return (GetResult) client.execute(request);
    }

    @Override
    public PutResult put(PutRequest request) {
        checkClient();
        return (PutResult) client.execute(request);
    }

    @Override
    public WriteMultipleResult writeMultiple(WriteMultipleRequest request) {
        checkClient();
        return (WriteMultipleResult) client.execute(request);
    }

    @Override
    public MultiDeleteResult multiDelete(MultiDeleteRequest request) {
        checkClient();
        return (MultiDeleteResult) client.execute(request);
    }

    @Override
    public QueryResult query(QueryRequest request) {
        checkClient();
        return (QueryResult) client.execute(request);
    }

    @Override
    public QueryIterableResult queryIterable(QueryRequest request) {
        checkClient();
        return new QueryIterableResult(request, this);
    }

    @Override
    public PrepareResult prepare(PrepareRequest request) {
        checkClient();
        return (PrepareResult) client.execute(request);
    }

    @Override
    public TableResult tableRequest(TableRequest request) {
        checkClient();
        TableResult res = (TableResult) client.execute(request);
        /* update rate limiters, if table has limits */
        client.updateRateLimiters(res.getTableName(), res.getTableLimits());
        return res;
    }

    @Override
    public TableResult getTable(GetTableRequest request) {
        checkClient();
        TableResult res = (TableResult) client.execute(request);
        /* update rate limiters, if table has limits */
        client.updateRateLimiters(res.getTableName(), res.getTableLimits());
        return res;
    }

    @Override
    public SystemResult systemRequest(SystemRequest request) {
        checkClient();
        return (SystemResult) client.execute(request);
    }

    @Override
    public SystemResult systemStatus(SystemStatusRequest request) {
        checkClient();
        return (SystemResult) client.execute(request);
    }

    @Override
    public TableUsageResult getTableUsage(TableUsageRequest request) {
        checkClient();
        return (TableUsageResult) client.execute(request);
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest request) {
        checkClient();
        return (ListTablesResult) client.execute(request);
    }

    @Override
    public GetIndexesResult getIndexes(GetIndexesRequest request) {
        checkClient();
        return (GetIndexesResult) client.execute(request);
    }

    @Override
    public TableResult addReplica(AddReplicaRequest request) {
        checkClient();
        return (TableResult) client.execute(request);
    }

    @Override
    public TableResult dropReplica(DropReplicaRequest request) {
        checkClient();
        return (TableResult) client.execute(request);
    }

    @Override
    public ReplicaStatsResult getReplicaStats(ReplicaStatsRequest request) {
        checkClient();
        return (ReplicaStatsResult) client.execute(request);
    }

    @Override
    synchronized public void close() {
        checkClient();
        client.shutdown();
        client = null;
    }

    /**
     * Returns the namespaces in a store as an array of String.
     *
     * @return the namespaces, or null if none are found
     */
    @Override
    public String[] listNamespaces() {
        SystemResult dres = doSystemRequest("show as json namespaces");

        String jsonResult = dres.getResultString();
        if (jsonResult == null) {
            return null;
        }
        MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

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
    }

    /**
     * Returns the users in a store as an array of {@link UserInfo}.
     *
     * @return the list of users or null if none are found
     */
    @Override
    public UserInfo[] listUsers() {
        SystemResult dres = doSystemRequest("show as json users");

        String jsonResult = dres.getResultString();
        if (jsonResult == null) {
            return null;
        }

        MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

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
    }

    /**
     * Returns the roles in a store as an array of String.
     *
     * @return the roles or null if none are found
     */
    @Override
    public String[] listRoles() {
        SystemResult dres = doSystemRequest("show as json roles");

        String jsonResult = dres.getResultString();
        if (jsonResult == null) {
            return null;
        }
        MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

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
        return client.getStatsControl();
    }

    @Override
    public TableResult enableChangeStreaming(String tableName,
                                             String compartmentId,
                                             boolean enabled,
                                             int timeoutMs,
                                             int pollIntervalMs) {
        TableRequest req = new TableRequest()
            .setTableName(tableName)
            .setChangeStreamingEnabled(enabled);
        if (compartmentId != null) {
            req.setCompartment(compartmentId);
        }
        try {
            TableResult res = tableRequest(req);
            if (res == null) {
                throw new IllegalStateException("No response from server for Change Streaming operation");
            }
            res.waitForCompletion(this, timeoutMs, pollIntervalMs);
            return res;
        } catch (Exception e) {
            if (e.toString().contains("must have either statement or limits")) {
                throw new OperationNotSupportedException("Change Streaming not supported by server");
            }
            throw e;
       }
    }

    /**
     * Ensure that the client exists and hasn't been closed;
     */
    private void checkClient() {
        if (client == null) {
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
