/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
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

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    private final NoSQLHandleAsync client;

    public NoSQLHandleImpl(NoSQLHandleConfig config) {
        client = new NoSQLHandleAsyncImpl(config);
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
                /*sigProvider.setOnSignatureRefresh(new SigRefresh());
                client.createAuthRefreshList();*/
            }
        }
    }

    @Override
    public DeleteResult delete(DeleteRequest request) {
        checkClient();
        return Mono.from(client.delete(request)).block();
    }

    @Override
    public GetResult get(GetRequest request) {
        checkClient();
        return Mono.from(client.get(request)).block();
    }

    @Override
    public PutResult put(PutRequest request) {
        checkClient();
        return Mono.from(client.put(request)).block();
    }

    @Override
    public WriteMultipleResult writeMultiple(WriteMultipleRequest request) {
        checkClient();
        return Mono.from(client.writeMultiple(request)).block();
    }

    @Override
    public MultiDeleteResult multiDelete(MultiDeleteRequest request) {
        checkClient();
        return Mono.from(client.multiDelete(request)).block();
    }

    @Override
    public QueryResult query(QueryRequest request) {
        checkClient();
        return Flux.from(client.query(request)).next().block();
    }

    @Override
    public QueryIterableResult queryIterable(QueryRequest request) {
        checkClient();
        return new QueryIterableResult(request, this);
    }

    @Override
    public PrepareResult prepare(PrepareRequest request) {
        checkClient();
        return Mono.from(client.prepare(request)).block();
    }

    @Override
    public TableResult tableRequest(TableRequest request) {
        checkClient();
        TableResult res =Mono.from(client.tableRequest(request)).block();
        /* update rate limiters, if table has limits */
        //client.updateRateLimiters(res.getTableName(), res.getTableLimits());
        return res;
    }

    @Override
    public TableResult getTable(GetTableRequest request) {
        checkClient();
        TableResult res = Mono.from(client.getTable(request)).block();
        /* update rate limiters, if table has limits */
        //client.updateRateLimiters(res.getTableName(), res.getTableLimits());
        return res;
    }

    @Override
    public SystemResult systemRequest(SystemRequest request) {
        checkClient();
        return Mono.from(client.systemRequest(request)).block();
    }

    @Override
    public SystemResult systemStatus(SystemStatusRequest request) {
        checkClient();
        return Mono.from(client.systemStatus(request)).block();
    }

    @Override
    public TableUsageResult getTableUsage(TableUsageRequest request) {
        checkClient();
        return Mono.from(client.getTableUsage(request)).block();
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest request) {
        checkClient();
        return Mono.from(client.listTables(request)).block();
    }

    @Override
    public GetIndexesResult getIndexes(GetIndexesRequest request) {
        checkClient();
        return Mono.from(client.getIndexes(request)).block();
    }

    @Override
    public TableResult addReplica(AddReplicaRequest request) {
        checkClient();
        return Mono.from(client.addReplica(request)).block();
    }

    @Override
    public TableResult dropReplica(DropReplicaRequest request) {
        checkClient();
        return Mono.from(client.dropReplica(request)).block();
    }

    @Override
    public ReplicaStatsResult getReplicaStats(ReplicaStatsRequest request) {
        checkClient();
        return Mono.from(client.getReplicaStats(request)).block();
    }

    @Override
    synchronized public void close() {
        checkClient();
        //client.shutdown();
        //client = null;
    }

    /**
     * Returns the namespaces in a store as an array of String.
     *
     * @return the namespaces, or null if none are found
     */
    @Override
    public String[] listNamespaces() {
        List<String> list =
                Flux.from(client.listNamespaces()).collectList().block();
        if(list == null) {
            return null;
        }
        return list.toArray(new String[0]);
    }

    /**
     * Returns the users in a store as an array of {@link UserInfo}.
     *
     * @return the list of users or null if none are found
     */
    @Override
    public UserInfo[] listUsers() {
        List<UserInfo> list =
                Flux.from(client.listUsers()).collectList().block();
        if(list == null) {
            return null;
        }
        return list.toArray(new UserInfo[0]);
    }

    /**
     * Returns the roles in a store as an array of String.
     *
     * @return the roles or null if none are found
     */
    @Override
    public String[] listRoles() {
        List<String> list =
                Flux.from(client.listRoles()).collectList().block();
        if(list == null) {
            return null;
        }
        return list.toArray(new String[0]);
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
        checkClient();
       return Mono.from(client.doTableRequest(request, Duration.ofMillis(timeoutMs),
               Duration.ofMillis(pollIntervalMs))).block();
    }

    @Override
    public SystemResult doSystemRequest(String statement,
                                        int timeoutMs,
                                        int pollIntervalMs) {
        checkClient();
        SystemRequest dreq =
            new SystemRequest().setStatement(statement.toCharArray());
        return Mono.from(client.doSystemRequest(dreq, Duration.ofMillis(timeoutMs),
                Duration.ofMillis(pollIntervalMs))).block();
    }

    @Override
    public StatsControl getStatsControl() {
        return null;
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
        return null;
    }

    /**
     * @hidden
     * For testing use
     */
    public short getSerialVersion() {
        return ((NoSQLHandleAsyncImpl)client).getSerialVersion();
    }

    /**
     * @hidden
     *
     * Testing use only.
     */
    public void setDefaultNamespace(String ns) {
        ((NoSQLHandleAsyncImpl)client).setDefaultNamespace(ns);
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
            //client.doRefresh(refreshMs);
        }
    }
}
