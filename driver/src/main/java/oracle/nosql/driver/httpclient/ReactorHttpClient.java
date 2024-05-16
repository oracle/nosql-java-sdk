package oracle.nosql.driver.httpclient;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.logging.Logger;

import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;

public class ReactorHttpClient {
    private static Logger logger =
            Logger.getLogger(ReactorHttpClient.class.getName());
    private final String host;
    private final int port;
    private final SslContext sslContext;
    private final ConnectionPoolConfig connectionPoolConfig;

    private final reactor.netty.http.client.HttpClient httpClient;

    private ReactorHttpClient(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.connectionPoolConfig = builder.config;
        this.sslContext = builder.sslContext;

        ConnectionProvider connectionProvider = (connectionPoolConfig.getMaxConnections() == 1) ?
                ConnectionProvider.newConnection() :
                ConnectionProvider
                .builder(this.host + ":" + this.port + "-" + "pool")
                .maxConnections(connectionPoolConfig.getMaxConnections())
                .pendingAcquireTimeout(Duration.ofSeconds(connectionPoolConfig.getPendingAcquireTimeout()))
                .pendingAcquireMaxCount(connectionPoolConfig.getMaxPendingAcquires())
                .maxIdleTime(Duration.ofSeconds(connectionPoolConfig.getMaxIdleTime()))
                .build();

        httpClient = HttpClient
                .create(connectionProvider)
                .host(this.host)
                .port(this.port)
                //.wiretap("reactor.netty.http.client.HttpClient", LogLevel.DEBUG, AdvancedByteBufFormat.TEXTUAL, StandardCharsets.UTF_8)
                .wiretap(true);
                /*.httpResponseDecoder(spec ->
                        spec.maxInitialLineLength(builder.maxInitialLineLength)
                            .maxHeaderSize(builder.maxHeaderSize)
                            .maxChunkSize(builder.maxChunkSize)
                );*/
        if(sslContext != null) {
            httpClient.secure(sslContextSpec -> sslContextSpec.sslContext(sslContext));
        }
        httpClient.warmup().subscribe();
    }

    public Mono<HttpResponse> sendRequest(String uri, HttpHeaders headers, HttpMethod httpMethod, ByteBuf body) {
        return httpClient.request(httpMethod)
                .uri(uri)
                .send((httpClientRequest, nettyOutbound) -> {
                    httpClientRequest.headers(headers);
                    if(body != null) {
                        return nettyOutbound.send(Mono.just(body));
                    }
                    return nettyOutbound;
                })
                .responseConnection((httpClientResponse, connection) -> {
                    HttpResponse response = new HttpResponse(httpClientResponse,connection);
                    return Mono.just(response);
                })
                .single();
    }

    public Mono<HttpResponse> getRequest(String uri, HttpHeaders headers) {
        return sendRequest(uri, headers, HttpMethod.GET, null);
    }

    public Mono<HttpResponse> postRequest(String uri, HttpHeaders headers, ByteBuf body) {
        return sendRequest(uri, headers, HttpMethod.POST, body);
    }

    public Mono<HttpResponse> postRequest(String uri, Mono<HttpHeaders> headers,
                                          ByteBuf body) {
        return headers.flatMap(h -> sendRequest(uri, h, HttpMethod.POST, body));
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    // Add other methods for different HTTP methods as needed

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String host = "localhost";
        private int port = 8080;

        private int maxContentLength = 32 * 1024 * 1024; // 32MB

        private int maxInitialLineLength = 4096;

        private int maxHeaderSize = 8192;
        private int maxChunkSize = 65536;

        private int handshakeTimeoutMs = 3000;

        private ConnectionPoolConfig config = ConnectionPoolConfig.builder().build();

        private SslContext sslContext;


        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

       public Builder maxContentLength(int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return this;
       }

       public Builder maxInitialLineLength(int maxInitialLineLength) {
            this.maxInitialLineLength = maxInitialLineLength;
            return this;
       }

        public Builder maxHeaderSize(int maxHeaderSize) {
            this.maxHeaderSize = maxHeaderSize;
            return this;
        }
       
       public Builder maxChunkSize(int maxChunkSize) {
            this.maxChunkSize = maxChunkSize;
            return this;
       }

       public Builder handshakeTimeoutMs(int handshakeTimeoutMs) {
            this.handshakeTimeoutMs = handshakeTimeoutMs;
            return this;
       }

        public Builder config(ConnectionPoolConfig config) {
            this.config = config;
            return this;
        }

        public Builder sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public ReactorHttpClient build() {
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("host is required");
            }
            if(port == 0) {
                throw new IllegalArgumentException("port is required");
            }
            if (config == null) {
                config = new ConnectionPoolConfig.Builder().build(); // Use default config
            }
            return new ReactorHttpClient(this);
        }
    }

    public static ReactorHttpClient createMinimalClient(String host,
                                                        int port,
                                                        SslContext sslCtx,
                                                        int handshakeTimeoutMs,
                                                        String name) {
        return ReactorHttpClient.builder()
                .host(host)
                .port(port)
                .sslContext(sslCtx)
                .config(ConnectionPoolConfig.builder()
                        .maxConnections(1).build())
                .build();
    }
}

