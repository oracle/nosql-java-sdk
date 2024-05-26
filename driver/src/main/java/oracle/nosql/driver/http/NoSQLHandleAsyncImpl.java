package oracle.nosql.driver.http;

import io.netty.handler.ssl.SslContextBuilder;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.NoSQLHandleConfig;
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.logging.Logger;

public class NoSQLHandleAsyncImpl implements NoSQLHandleAsync {
    private final AsyncClient client;

    public NoSQLHandleAsyncImpl(NoSQLHandleConfig config) {
        final Logger logger = getLogger(config);

        /*
         * config SslContext first, on-prem authorization provider
         * will reuse the context in NoSQLHandleConfig
         */
        configSslContext(config);

        client = new AsyncClient(config,logger);

        /* configAuthProvider may use client */
        configAuthProvider(logger, config);
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
            stProvider.prepare();
        } else if (ap instanceof SignatureProvider) {
            SignatureProvider sigProvider = (SignatureProvider) ap;
            if (sigProvider.getLogger() == null) {
                sigProvider.setLogger(logger);
            }
            sigProvider.prepare(config);
            /*if (config.getAuthRefresh()) {
                sigProvider.setOnSignatureRefresh(new NoSQLHandleImpl.SigRefresh());
                client.createAuthRefreshList();
            }*/
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
        return Logger.getLogger(getClass().getName());
    }

    @Override
    public Mono<GetResult> get(GetRequest request) {
        return client.execute(request).cast(GetResult.class);
    }

    @Override
    public Publisher<PutResult> put(PutRequest request) {
        return client.execute(request).cast(PutResult.class);
    }

    @Override
    public Publisher<DeleteResult> delete(DeleteRequest request) {
        return client.execute(request).cast(DeleteResult.class);
    }

    @Override
    public Mono<TableResult> getTable(GetTableRequest request) {
        return client.execute(request).cast(TableResult.class);
    }

    @Override
    public Publisher<TableResult> tableRequest(TableRequest request) {
        return client.execute(request).cast(TableResult.class);
    }

    @Override
    public Publisher<ListTablesResult> listTables(ListTablesRequest request) {
        return client.execute(request).cast(ListTablesResult.class);
    }

    @Override
    public Publisher<QueryResult> query(QueryRequest request) {
        return client.executeQuery(request);
    }

    @Override
    public Publisher<MapValue> queryIterable(QueryRequest request) {
        return Flux.from(query(request)).flatMap(queryResult ->
           Flux.fromIterable(queryResult.getResults()));
    }

    @Override
    public Publisher<PrepareResult> prepare(PrepareRequest request) {
        return client.execute(request).cast(PrepareResult.class);
    }

    @Override
    public Publisher<WriteMultipleResult> writeMultiple(WriteMultipleRequest request) {
        return client.execute(request).cast(WriteMultipleResult.class);
    }

    @Override
    public Publisher<MultiDeleteResult> multiDelete(MultiDeleteRequest request) {
        return client.execute(request).cast(MultiDeleteResult.class);
    }

    @Override
    public Publisher<SystemResult> systemRequest(SystemRequest request) {
        return client.execute(request).cast(SystemResult.class);
    }

    @Override
    public Publisher<SystemResult> systemStatus(SystemStatusRequest request) {
        return client.execute(request).cast(SystemResult.class);
    }

    @Override
    public Publisher<TableUsageResult> getTableUsage(TableUsageRequest request) {
        return client.execute(request).cast(TableUsageResult.class);
    }

    @Override
    public Publisher<GetIndexesResult> getIndexes(GetIndexesRequest request) {
        return client.execute(request).cast(GetIndexesResult.class);
    }

    @Override
    public Publisher<TableResult> doTableRequest(TableRequest request,
                                                 Duration timeout,
                                                 Duration pollInterval) {
        return Mono.from(tableRequest(request))
            .flatMap(result ->
                Mono.from(result.waitForCompletionAsync(this,
                    timeout, pollInterval)
                )
                .then(Mono.fromCallable(() -> result))
            );
    }

    @Override
    public Publisher<SystemResult> doSystemRequest(SystemRequest request,
                                                   Duration timeout,
                                                   Duration pollInterval) {
        return Mono.from(systemRequest(request))
            .flatMap(result -> Mono.from(result.waitForCompletionAsync(
                        this, timeout, pollInterval))
                .then(Mono.fromCallable(() -> result))
            );
    }

    @Override
    public Publisher<String> listNamespaces() {
        Mono<SystemResult> systemResultMono = doSystemRequest(
                "show as json namespaces");
        return systemResultMono.flatMapMany(dres -> {
            String jsonResult = dres.getResultString();
            if (jsonResult == null) {
                return Flux.empty();
            }
            MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

            FieldValue namespaces = root.get("namespaces");
            if (namespaces == null) {
                return Flux.empty();
            }


            ArrayList<String> results = new ArrayList<>(
                    namespaces.asArray().size());
            for (FieldValue val : namespaces.asArray()) {
                results.add(val.getString());
            }
            return Flux.fromIterable(results);
        });
    }

    @Override
    public Publisher<String> listRoles() {
        Mono<SystemResult> systemResultMono = doSystemRequest(
                "show as json roles");
        return systemResultMono.flatMapMany(dres -> {
            String jsonResult = dres.getResultString();
            if (jsonResult == null) {
                return Flux.empty();
            }
            MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

            FieldValue roles = root.get("roles");
            if (roles == null) {
                return Flux.empty();
            }

            ArrayList<String> results = new ArrayList<>(roles.asArray().size());
            for (FieldValue val : roles.asArray()) {
                results.add(val.asMap().getString("name"));
            }
            return Flux.fromIterable(results);
        });
    }

    @Override
    public Publisher<UserInfo> listUsers() {
        Mono<SystemResult> systemResultMono = doSystemRequest(
                "show as json users");
        return systemResultMono.flatMapMany(dres -> {
            String jsonResult = dres.getResultString();
            if (jsonResult == null) {
                return Flux.empty();
            }

            MapValue root = JsonUtils.createValueFromJson(jsonResult, null).asMap();

            FieldValue users = root.get("users");
            if (users == null) {
                return Flux.empty();
            }

            ArrayList<UserInfo> results = new ArrayList<>(
                    users.asArray().size());

            for (FieldValue val : users.asArray()) {
                String id = val.asMap().getString("id");
                String name = val.asMap().getString("name");
                results.add(new UserInfo(id, name));
            }
            return Flux.fromIterable(results);
        });
    }

    @Override
    public Publisher<TableResult> addReplica(AddReplicaRequest request) {
        return client.execute(request).cast(TableResult.class);
    }

    @Override
    public Publisher<TableResult> dropReplica(DropReplicaRequest request) {
        return client.execute(request).cast(TableResult.class);
    }

    @Override
    public Publisher<ReplicaStatsResult> getReplicaStats(ReplicaStatsRequest request) {
        return client.execute(request).cast(ReplicaStatsResult.class);
    }


    @Override
    public void close() {
        client.shutdown();
    }

    private Mono<SystemResult> doSystemRequest(String statement) {
        SystemRequest systemRequest = new SystemRequest()
            .setStatement(statement.toCharArray());
        return Mono.from(doSystemRequest(systemRequest,
                Duration.ofSeconds(30), Duration.ofSeconds(1)));
    }

    short getSerialVersion() {
       return client.getSerialVersion();
    }

    void setDefaultNamespace(String ns) {
        client.setDefaultNamespace(ns);
    }
}
