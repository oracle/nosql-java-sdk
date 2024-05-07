package oracle.nosql.driver.http;

import io.netty.handler.ssl.SslContextBuilder;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLException;
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

        client = new AsyncClient(config);

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
        Logger logger = Logger.getLogger(getClass().getName());
        return logger;
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
        return client.doTableRequest(request);
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
    public void close() {
        client.shutdown();
    }
}
