/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.ProxyProvider;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Internal use only, not meant for public usage can change in the future.
 * <p>
 * The HttpClient which uses {@link HttpClient}.
 * <p>
 *
 * The reactor netty {@link HttpClient} shares LoopResource(EventLoopGroup)
 * and ConnectionPool among different {@link HttpClient} instances if created
 * without providing any of them. The default LoopResource and ConnectionPool
 * is expected to last for the application lifetime. If a HttpClient is
 * created using default resources it's dispose should not free the
 * underlying resources. If HttpClient is created with non default
 * ConnectionProvider it's dispose must free the ConnectionProvider also.
 * <p>
 * Reactor netty recommend to share common LoopResource among different
 * clients as it is non-blocking and costly to create. So default
 * LoopResource created by netty will be shared with all the created clients.
 * <p>
 * Only for data path request a separate connection pool is created. Metadata
 * requests like token request are processed without connection pool as a
 * MinimalClient.
 */
public class ReactorHttpClient {
    static final int DEFAULT_MAX_CONTENT_LENGTH = 32 * 1024 * 1024; // 32MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 3000;
    static final int DEFAULT_MAX_INITIAL_LINE_LENGTH = 4096;
    static final int DEFAULT_MAX_HEADER_SIZE = 8192;

    private final Logger logger;
    private final String host;
    private final int port;
    private final SslContext sslContext;
    private final ConnectionPoolConfig connectionPoolConfig;
    private final int maxContentLength;
    private final reactor.netty.http.client.HttpClient httpClient;
    private final String proxyHost;
    private final int proxyPort;
    private final String proxyUser;
    private final String proxyPassword;

    /*
     * TODO - Currently number of worker threads in EventLoopGroup uses
     *  default value which is number of available processors with minimum
     *  value of 4. There is no way to configure this using value of
     *  NoSQLHandleConfig.numThreads because we're sharing the default
     *  EventLoopGroup created by reactor netty and not providing new
     *  EventLoopGroup for each created client. Don't know whether to
     *  deprecate NoSQLHandleConfig.numThreads or add functionality to accept
     *  number of threads. Workaround is to use -Dreactor.netty.ioWorkerCount
     *
     */
    private ReactorHttpClient(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.connectionPoolConfig = builder.config;
        this.sslContext = builder.sslContext;
        this.logger = builder.logger;
        this.maxContentLength = builder.maxContentLength;
        this.proxyHost = builder.proxyHost;
        this.proxyPort = builder.proxyPort;
        this.proxyUser = builder.proxyUserName;
        this.proxyPassword = builder.proxyPassword;

        ConnectionProvider connectionProvider = (connectionPoolConfig.getMaxConnections() == 1) ?
            ConnectionProvider.newConnection() :
            ConnectionProvider
            .builder(this.host + ":" + this.port + "-" + "pool")
            .maxConnections(connectionPoolConfig.getMaxConnections())
            .pendingAcquireTimeout(Duration.ofSeconds(connectionPoolConfig.getPendingAcquireTimeout()))
            .pendingAcquireMaxCount(connectionPoolConfig.getMaxPendingAcquires())
            .maxIdleTime(Duration.ofSeconds(connectionPoolConfig.getMaxIdleTime()))
            .maxLifeTime(Duration.ofMillis(connectionPoolConfig.getMaxLifetime()))
            .build();

        HttpClient client = HttpClient
            .create(connectionProvider)
            .host(this.host)
            .port(this.port)
            // Decoder config
            .httpResponseDecoder(spec -> {
                return spec.maxChunkSize(builder.maxChunkSize)
                        .maxHeaderSize(builder.maxHeaderSize)
                        .maxInitialLineLength(builder.maxInitialLineLength);
            })
            // TCP configs. TODO is this required?
            .option(ChannelOption.SO_KEEPALIVE,true)
            .option(ChannelOption.TCP_NODELAY, true)
            //Enable wiretap
            .wiretap(true);

        // Proxy settings
        if (builder.proxyHost != null) {
            client = client.proxy(spec -> spec.type(ProxyProvider.Proxy.HTTP)
                .host(builder.proxyHost)
                .port(builder.proxyPort)
                .username(builder.proxyUserName)
                .password(pwd -> builder.proxyPassword));
        }

        // SSL config
        if (sslContext != null) {
            client = client.secure(sslContextSpec -> {
                sslContextSpec.sslContext(sslContext)
                .handshakeTimeoutMillis(builder.sslHandshakeTimeoutMs);
            });
        }
        httpClient = client;
        httpClient.warmup().subscribe();
    }

    /**
     * Send the Http request to the remote server
     * @param uri  The target address to send the request
     * @param headers  The HTTP header to use with the request
     * @param httpMethod The HTTP request method
     * @param body The request body
     * @return Mono of the response from the server
     */
    public Mono<HttpResponse> sendRequest(String uri, HttpHeaders headers, HttpMethod httpMethod, ByteBuf body) {
        /* Reference to current HttpResponse. This reference is needed when
         * response publisher is not consumed we need to drain the response
         * ByteBuf.
         */
        final AtomicReference<HttpResponse> responseReference = new AtomicReference<>();

        return httpClient.request(httpMethod)
            .uri(uri)
            .send((httpClientRequest, nettyOutbound) -> {
                httpClientRequest.headers(headers);
                if (body != null) {
                    return nettyOutbound.send(Mono.just(body));
                }
                return nettyOutbound;
            })
            .responseConnection((httpClientResponse, connection) -> {
                HttpResponse response = new HttpResponse(httpClientResponse,connection);
                responseReference.set(response);
                return Mono.just(response);
            })
            .next()
            .doOnCancel(() -> {
                HttpResponse response = responseReference.get();
                if (response != null) {
                    response.releaseUnSubscribedResponse(HttpResponse.SubscriptionState.CANCELLED);
                }
            })
            .onErrorMap(throwable -> {
                HttpResponse response = responseReference.get();
                if (response != null) {
                    response.releaseUnSubscribedResponse(HttpResponse.SubscriptionState.ERROR);
                }
                return throwable;
            });
    }

    /**
     * send HTTP GET request
     *
     * @param uri The target address to send the request
     * @param headers The HTTP header to use with the request
     * @return Mono of the response from the server
     */
    public Mono<HttpResponse> getRequest(String uri, HttpHeaders headers) {
        return sendRequest(uri, headers, HttpMethod.GET, null);
    }

    /**
     * send HTTP PUT request
     *
     * @param uri The target address to send the request
     * @param headers The HTTP header to use with the request
     * @param body The request body
     * @return Mono of the response from the server
     */
    public Mono<HttpResponse> postRequest(String uri, HttpHeaders headers, ByteBuf body) {
        return sendRequest(uri, headers, HttpMethod.POST, body);
    }

    /**
     * send HTTP PUT request
     *
     * @param uri The target address to send the request
     * @param headers A Mono which returns The HTTP header to use with the
     *                request when subscribed to
     * @param body The request body
     * @return Mono of the response from the server
     */
    public Mono<HttpResponse> postRequest(String uri, Mono<HttpHeaders> headers,
                                          ByteBuf body) {
        return headers.flatMap(h -> sendRequest(uri, h, HttpMethod.POST, body));
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }


    /**
     * @hidden
     * For testing only
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * @hidden
     * For testing only
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * @hidden
     * For testing only
     */
    public String getHost() {
        return host;
    }

    /**
     * @hidden
     * For testing only
     */
    public int getPort() {
        return port;
    }


    /**
     * @hidden
     * For testing only
     */
    public SslContext getSslContext() {
        return sslContext;
    }

    /**
     * @hidden
     * For testing only
     */
    public ConnectionPoolConfig getConnectionPoolConfig() {
        return connectionPoolConfig;
    }

    /**
     * @hidden
     * For testing only
     */
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * @hidden
     * For testing only
     */
    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * @hidden
     * For testing only
     */
    public String getProxyUser() {
        return proxyUser;
    }

    /**
     * @hidden
     * For testing only
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    // Add other methods for different HTTP methods as needed

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String host = "localhost";
        private int port = 80;
        private int maxContentLength = DEFAULT_MAX_CONTENT_LENGTH;
        private int maxInitialLineLength = DEFAULT_MAX_INITIAL_LINE_LENGTH;
        private int maxHeaderSize = DEFAULT_MAX_HEADER_SIZE;
        private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
        private int sslHandshakeTimeoutMs = DEFAULT_HANDSHAKE_TIMEOUT_MS;
        private ConnectionPoolConfig config = ConnectionPoolConfig.builder().build();
        private SslContext sslContext;
        private String proxyHost;
        private int proxyPort;
        private String proxyUserName;
        private String proxyPassword;
        private Logger logger = Logger.getLogger(ReactorHttpClient.class.getName());

        /**
         * Sets the host name for the HTTP server.
         * Default to localhost.
         *
         * @param host the host name for the HTTP server
         * @return this
         * @throws IllegalArgumentException If host is null or empty
         */
        public Builder host(String host) {
            this.host = host;
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("host is either null or " +
                    "empty");
            }
            return this;
        }

        /**
         * Set the port for the HTTP server.
         * Default to 80
         *
         * @param port port for the HTTP server.
         * @return this
         * @throws IllegalArgumentException If port is not positive
         */
        public Builder port(int port) {
            this.port = port;
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            return this;
        }

        /**
         * Set the maximum size in bytes of requests/responses. If zero
         * default value is used.
         * Default to {@value DEFAULT_MAX_CONTENT_LENGTH}.
         *
         * @param maxContentLength maximum size in bytes of requests/responses.
         * @return this
         * @throws IllegalArgumentException If maxContentLength is negative
         */
        public Builder maxContentLength(int maxContentLength) {
            if (maxContentLength != 0) {
                this.maxContentLength = maxContentLength;
            }
            if (maxContentLength < 0) {
                throw new IllegalArgumentException("maxContentLength is " +
                    "negative");
            }
            return this;
       }

        /**
         * Set the maximum length that can be decoded for the HTTP request's
         * initial line. If zero default value is used.
         * Default to {@value DEFAULT_MAX_INITIAL_LINE_LENGTH}
         *
         * @param maxInitialLineLength the maximum initial line length
         * @return this
         * @throws IllegalArgumentException If maxInitialLineLength is negative
         */
       public Builder maxInitialLineLength(int maxInitialLineLength) {
           if (maxInitialLineLength != 0) {
               this.maxInitialLineLength = maxInitialLineLength;
           }
            if (maxInitialLineLength < 0) {
                throw new IllegalArgumentException("maxInitialLineLength is " +
                    "negative");
            }
            return this;
       }


        /**
         * Set the maximum header size that can be decoded for the HTTP
         * request. If zero default value is used.
         * Default to {@value DEFAULT_MAX_HEADER_SIZE}
         *
         * @param maxHeaderSize the maximum header size
         * @return this
         * @throws IllegalArgumentException If maxHeaderSize is negative
         */
        public Builder maxHeaderSize(int maxHeaderSize) {
            if (maxHeaderSize != 0) {
                this.maxHeaderSize = maxHeaderSize;
            }
            if (maxHeaderSize < 0) {
                throw new IllegalArgumentException("maxHeaderSize is negative");
            }
            return this;
        }

        /**
         * Set the maximum chunk size that can be decoded for the HTTP request.
         * If zero default value is used.
         * Default to {@value DEFAULT_MAX_CHUNK_SIZE}
         *
         * @param maxChunkSize the maximum chunk size
         * @return this
         * @throws IllegalArgumentException If maxChunkSize is negative
         */
       public Builder maxChunkSize(int maxChunkSize) {
           if (maxChunkSize != 0) {
               this.maxChunkSize = maxChunkSize;
           }
            if (maxChunkSize < 0) {
                throw new IllegalArgumentException("maxChunkSize must be " +
                    "positive");
            }
            return this;
       }

        /**
         * Set the SSL handshake timeout. If zero default value is used.
         * Default to {@value DEFAULT_HANDSHAKE_TIMEOUT_MS}
         *
         * @param sslHandshakeTimeoutMs the SSL handshake timeout in milliseconds
         * @return this
         * @throws IllegalArgumentException If sslHandshakeTimeoutMs is not
         * positive
         */
       public Builder sslHandshakeTimeoutMs(int sslHandshakeTimeoutMs) {
           if (sslHandshakeTimeoutMs != 0) {
               this.sslHandshakeTimeoutMs = sslHandshakeTimeoutMs;
           }
           if (sslHandshakeTimeoutMs < 0) {
               throw new IllegalArgumentException("sslHandshakeTimeoutMs is " +
                   "negative");
           }
            return this;
       }

        /**
         * sets the connection pool config
          *
         * @param config connection pool config
         * @return this
         */
       public Builder connectionPoolConfig(ConnectionPoolConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Set the ssl context
         *
         * @param sslContext the ssl context
         * @return this
         */
        public Builder sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * Set the proxy host to use
         *
         * @param proxyHost proxy host
         * @return this
         */
        public Builder proxyHost(String proxyHost) {
            this.proxyHost = proxyHost;
            return this;
        }

        /**
         * set the proxy port to use
         *
         * @param proxyPort proxy port
         * @return this
         */
        public Builder proxyPort(int proxyPort) {
            this.proxyPort = proxyPort;
            return this;
        }

        /**
         * Set the proxy user name to use
         *
         * @param proxyUserName proxy user name
         * @return this
         */
        public Builder proxyUsername(String proxyUserName) {
            this.proxyUserName = proxyUserName;
            return this;
        }

        /**
         * Set the proxy password to use
         *
         * @param proxyPassword proxy password
         * @return this
         */
        public Builder proxyPassword(String proxyPassword) {
            this.proxyPassword = proxyPassword;
            return this;
        }

        /**
         * Set the logger to use
         *
         * @param logger logger
         * @return this
         */
        public Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public ReactorHttpClient build() {
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("host is required");
            }
            if (port == 0) {
                throw new IllegalArgumentException("port is required");
            }
            if ((proxyHost != null && proxyPort == 0) ||
                    (proxyHost == null && proxyPort != 0)) {
                throw new IllegalArgumentException(
                        "To configure an HTTP proxy, both host and port are required");
            }
            if ((proxyUserName != null && proxyPassword == null) ||
                    (proxyUserName == null && proxyPassword != null)) {
                throw new IllegalArgumentException(
                        "To configure HTTP proxy authentication, both user name and " +
                                "password are required");
            }
            if (config == null) {
                config = new ConnectionPoolConfig.Builder().build(); // Use default config
            }
            return new ReactorHttpClient(this);
        }
    }

    /**
     * Create a HTTP client without connection pooling
     * @param host host to connect
     * @param port port to connect
     * @param sslCtx ssl context
     * @param handshakeTimeoutMs ssl handshake timeout in milliseconds
     * @param name name for the client
     * @param logger logger to use
     * @return new ReactorHttpClient
     */
    public static ReactorHttpClient createMinimalClient(String host,
                                                        int port,
                                                        SslContext sslCtx,
                                                        int handshakeTimeoutMs,
                                                        String name,
                                                        Logger logger) {
        return ReactorHttpClient.builder()
            .host(host)
            .port(port)
            .sslContext(sslCtx)
            .sslHandshakeTimeoutMs(handshakeTimeoutMs)
            .connectionPoolConfig(ConnectionPoolConfig.builder().maxConnections(1).build())
            .logger(logger)
            .build();
    }
}

