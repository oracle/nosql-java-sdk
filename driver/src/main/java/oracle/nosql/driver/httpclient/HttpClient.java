/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.driver.util.HttpConstants.CONNECTION;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
/*
 * If this code is ever made generic, the proxy information obtained
 * from this config needs to be abstracted to a generic class.
 */
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FutureListener;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.util.ConcurrentUtil;

/**
 * Netty HTTP client. Initialization process:
 * <p>
 * <ol>
 *   <li>create event loop for handling connections and requests. Assign it
 * a number of threads.</li>
 *   <li>bootstrap a client, setting the event loop group, socket options, and
 * remote address.</li>
 *   <li>create a ChannelPoolHandler instance to handle callback events from
 * a ChannelPool. The pool is used to allow the client to create new
 * connections on demand if one is busy. HTTP/1.1 doesn't allow concurrent
 * use of a single connection. This handler must be prepared to initialize
 * each new connection on creation.</li>
 *   <li>create a ChannelPool instance used to acquire and release channels for
 * use by requests.</li>
 * </ol>
 * <p>
 * Using the client to send request. The
 * request must be an instance of HttpRequest:
 * <ol>
 *     <li> Create a Netty HttpRequest. </li>
 *     <li>
 *         Call runRequest to send the request.<br/>
 *         <code>
 *         CompletableFuture<FullHttpResponse> response =
 *             httpClient.runRequest(request, timeoutMs);
 *         </code>
 *     </li>
 *     <li>
 *         For synchronous calls, wait for a response:<br/>
 *         <code>
 *             response.join() or response.get();
 *         </code>
 *     </li>
 *     <li>
 *          For asynchronous calls, consume the response future.
 *     </li>
 *     <li>
 *         If there was a problem with the send or receive, future completes
 *         with exception.
 *     </li>
 * </ol>
 * <p>
 */
public class HttpClient {

    static final int DEFAULT_MAX_CONTENT_LENGTH = 32 * 1024 * 1024; // 32MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 3000;
    static final int DEFAULT_MIN_POOL_SIZE = 2; // min pool size

    /* AttributeKey to attach a CompletableFuture to the Channel,
     * allowing the HttpResponseHandler to signal completion.
     */
    public static final AttributeKey<CompletableFuture<FullHttpResponse>>
        STATE_KEY = AttributeKey.valueOf("rqstate");

    //private final FixedChannelPool pool;
    private final ConnectionPool pool;
    private final HttpClientChannelPoolHandler poolHandler;

    private final int maxContentLength;
    private final int maxChunkSize;

    private final String host;
    private final int port;
    private final String name;

    /*
     * Amount of time to wait for acquiring a channel before timing
     * out and possibly retrying
     */
    private final int acquireRetryIntervalMs;

    /*
     * Non-null if using SSL
     */
    private final SslContext sslCtx;
    private final int handshakeTimeoutMs;

    /* Enable endpoint identification by default if using SSL */
    private boolean enableEndpointIdentification = true;

    private final Logger logger;

    /*
     * Proxy configuration
     */
    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;

    /*
     * May want boss and worker groups at some point
     */
    final NioEventLoopGroup workerGroup;

    /**
     * Creates a minimal HttpClient instance that is configured for
     * single-use or minimal use without concurrency.
     *
     * @param host the hostname for the HTTP server
     * @param port the port for the HTTP server
     * @param sslCtx if non-null, SSL context to use for connections.
     * @param handshakeTimeoutMs if not zero, timeout to use for SSL handshake
     * @param name A name to use in logging messages for this client.
     * @param logger A logger to use for logging messages.
     */
    public static HttpClient createMinimalClient(String host,
                                                 int port,
                                                 SslContext sslCtx,
                                                 int handshakeTimeoutMs,
                                                 String name,
                                                 Logger logger) {
        return new HttpClient(host,
                              port,
                              1, /* nThreads */
                              0, /* pool min */
                              0, /* pool inactivity period */
                              true, /* minimal client */
                              DEFAULT_MAX_CONTENT_LENGTH,
                              DEFAULT_MAX_CHUNK_SIZE,
                              sslCtx, handshakeTimeoutMs, name, logger);
    }

    /**
     * Creates a new HttpClient class capable of sending Netty HttpRequest
     * instances and receiving replies. This is a concurrent, asynchronous
     * interface capable of sending and receiving on multiple HTTP channels
     * at the same time.
     *
     * @param host the hostname for the HTTP server
     * @param port the port for the HTTP server
     * @param numThreads the number of async threads to use for Netty
     * notifications. If 0, a default value is used based on the number of
     * cores
     * @param connectionPoolMinSize the number of connections to keep in the
     * pool and keep alive using a minimal HTTP request. If 0, none are kept
     * alive
     * @param inactivityPeriodSeconds the number of seconds to keep an
     * inactive channel/connection before removing it. 0 means use the default,
     * a negative number means there is no timeout and channels are not
     * removed
     * @param maxContentLength maximum size in bytes of requests/responses.
     * If 0, a default value is used (32MB).
     * @param maxChunkSize maximum size in bytes of chunked response messages.
     * If 0, a default value is used (64KB).
     * @param sslCtx if non-null, SSL context to use for connections.
     * @param handshakeTimeoutMs if not zero, timeout to use for SSL handshake
     * @param name A name to use in logging messages for this client.
     * @param logger A logger to use for logging messages.
     */
    public HttpClient(String host,
                      int port,
                      int numThreads,
                      int connectionPoolMinSize,
                      int inactivityPeriodSeconds,
                      int maxContentLength,
                      int maxChunkSize,
                      SslContext sslCtx,
                      int handshakeTimeoutMs,
                      String name,
                      Logger logger) {

        this(host, port, numThreads, connectionPoolMinSize,
             inactivityPeriodSeconds, false /* not minimal */,
             maxContentLength, maxChunkSize, sslCtx, handshakeTimeoutMs, name, logger);
    }

    /*
     * Hidden/private to handle the minimal pool case
     */
    private HttpClient(String host,
                       int port,
                       int numThreads,
                       int connectionPoolMinSize,
                       int inactivityPeriodSeconds,
                       boolean isMinimalClient,
                       int maxContentLength,
                       int maxChunkSize,
                       SslContext sslCtx,
                       int handshakeTimeoutMs,
                       String name,
                       Logger logger) {

        this.logger = logger;
        this.sslCtx = sslCtx;
        this.host = host;
        this.port = port;
        this.name = name;

        this.maxContentLength = (maxContentLength == 0 ?
            DEFAULT_MAX_CONTENT_LENGTH : maxContentLength);
        this.maxChunkSize = (maxChunkSize == 0 ?
            DEFAULT_MAX_CHUNK_SIZE : maxChunkSize);

        this.handshakeTimeoutMs = (handshakeTimeoutMs == 0 ?
            DEFAULT_HANDSHAKE_TIMEOUT_MS : handshakeTimeoutMs);

        int cores = Runtime.getRuntime().availableProcessors();

        if (numThreads == 0) {
            numThreads = cores*2;
        }

        /* default pool min */
        if (connectionPoolMinSize == 0) {
            connectionPoolMinSize = DEFAULT_MIN_POOL_SIZE;
        } else if (connectionPoolMinSize < 0) {
            connectionPoolMinSize = 0; // no min size
        }

        workerGroup = new NioEventLoopGroup(numThreads);
        Bootstrap b = new Bootstrap();

        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.remoteAddress(host, port);

        poolHandler =
            new HttpClientChannelPoolHandler(this);
        pool = new ConnectionPool(b, poolHandler, logger,
                                  isMinimalClient,
                                  connectionPoolMinSize,
                                  inactivityPeriodSeconds);

        /*
         * Don't do keepalive if min size is not set. That configuration
         * doesn't care about keep connections alive. Also don't set for
         * minimal clients.
         */
        if (!isMinimalClient && connectionPoolMinSize > 0) {
            /* this is the main request client */
            pool.setKeepAlive(new ConnectionPool.KeepAlive() {
                    @Override
                    public boolean keepAlive(Channel ch) {
                        return doKeepAlive(ch);
                    }
                });
        }

        /* TODO: eventually add this to Config? */
        acquireRetryIntervalMs = Integer.getInteger(
                                    "oracle.nosql.driver.acquire.retryinterval",
                                    1000);
    }

    SslContext getSslContext() {
        return sslCtx;
    }

    public boolean isEndpointIdentificationEnabled() {
        return enableEndpointIdentification;
    }

    public void disableEndpointIdentification() {
        this.enableEndpointIdentification = false;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    String getName() {
        return name;
    }

    Logger getLogger() {
        return logger;
    }

    int getHandshakeTimeoutMs() {
        return handshakeTimeoutMs;
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public void configureProxy(NoSQLHandleConfig config) {
        proxyHost = config.getProxyHost();
        proxyPort = config.getProxyPort();
        proxyUsername = config.getProxyUsername();
        proxyPassword = config.getProxyPassword();
        if ((proxyHost != null && proxyPort == 0) ||
            (proxyHost == null && proxyPort != 0)) {
            throw new IllegalArgumentException(
                "To configure an HTTP proxy, both host and port are required");
        }
        if ((proxyUsername != null && proxyPassword == null) ||
            (proxyUsername == null && proxyPassword != null)) {
            throw new IllegalArgumentException(
                "To configure HTTP proxy authentication, both user name and " +
                "password are required");
        }
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public int getAcquiredChannelCount() {
        return pool.getAcquiredChannelCount();
    }

    public int getTotalChannelCount() {
        return pool.getTotalChannels();
    }

    public int getFreeChannelCount() {
        return pool.getFreeChannels();
    }

    /* available for testing */
    ConnectionPool getConnectionPool() {
        return pool;
    }

    /**
     * Cleanly shut down the client.
     */
    public void shutdown() {
        pool.close();
        /*
         * 0 means no quiet period, waiting for more tasks
         * 5000ms is total time to wait for shutdown (should never take this
         * long
         *
         * See doc:
         * https://netty.io/4.1/api/io/netty/util/concurrent/EventExecutorGroup.html#shutdownGracefully--
         */
        workerGroup.shutdownGracefully(0, 5000, TimeUnit.MILLISECONDS).
            syncUninterruptibly();
    }

    public void releaseChannel(Channel channel) {
        /* Clear any response handler state from channel before releasing it */
        channel.attr(STATE_KEY).set(null);

        /*
         * If channel is not healthy/active it will be closed and removed
         * from the pool. Don't wait for completion.
         */
        pool.release(channel);
    }

    /**
     * Close and remove channel from client pool.
     */
    public void removeChannel(Channel channel) {
        logFine(logger, "closing and removing channel " + channel);
        pool.removeChannel(channel);
    }


    /**
     * Sends an HttpRequest, setting up the ResponseHandler as the handler to
     * use for the (asynchronous) response.
     *
     * @param request the request
     *
     * @throws IOException if there is a network problem (bad channel). Such
     * exceptions can be retried.
     */
    public CompletableFuture<FullHttpResponse> runRequest(HttpRequest request,
                                                          int timeoutMs) {
        CompletableFuture<FullHttpResponse> responseFuture =
            new CompletableFuture<>();
        long deadlineNs = System.nanoTime() +
                          TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        pool.acquire().addListener((FutureListener<Channel>) channelFuture -> {
            if (channelFuture.isSuccess()) {
                Channel channel = channelFuture.getNow();

                if (isTimeout(deadlineNs)) {
                    String msg = "Timed out trying to acquire channel";
                    logInfo(logger, "HttpClient " + name + " " + msg);
                    /* release channel and request Bytebuf */
                    releaseChannel(channel);
                    ReferenceCountUtil.release(request);
                    responseFuture.completeExceptionally(
                        new TimeoutException(msg));
                    return;
                }

                long remainingTimeoutNs = deadlineNs - System.nanoTime();
                long remainingTimeoutMs = Math.max(1,
                    TimeUnit.NANOSECONDS.toMillis(remainingTimeoutNs));

                /* Execute the request on the acquired channel */
                CompletableFuture<FullHttpResponse> requestExecutionFuture =
                    runRequest(request, channel, remainingTimeoutMs);

                /* When the request execution future completes (either
                 * successfully or exceptionally),
                 * complete the public responseFuture and ensure the channel
                 * is released back to the pool.
                 */
                requestExecutionFuture.whenComplete((response, throwable) -> {
                    /* Always release the channel */
                    releaseChannel(channel);
                    if (throwable != null) {
                        responseFuture.completeExceptionally(throwable);
                    } else {
                        responseFuture.complete(response);
                    }
                });
            } else {
                /* channel acquisition failed */
                //TODO send proper exception
                responseFuture.completeExceptionally(channelFuture.cause());
            }
        });
        return responseFuture;
    }

    public CompletableFuture<FullHttpResponse> runRequest(HttpRequest request,
                                                          Channel channel,
                                                          long timeoutMs) {
        CompletableFuture<FullHttpResponse>
            responseFuture = new CompletableFuture<>();
        /* Attach the CompletableFuture to the channel's attributes */
        channel.attr(STATE_KEY).set(responseFuture);

        /* Add timeout handler to the pipeline */
        channel.pipeline().addFirst(
            new ReadTimeoutHandler(timeoutMs, TimeUnit.MILLISECONDS));

        /* Write the request to the channel and flush it */
        channel.writeAndFlush(request)
        .addListener((ChannelFutureListener) writeFuture -> {
            if (!writeFuture.isSuccess()) {
                /* If write fails, complete the future exceptionally */
                channel.attr(STATE_KEY).set(null);
                responseFuture.completeExceptionally(writeFuture.cause());
            }
        });
        return responseFuture;
    }

    private static boolean isTimeout(long deadlineNs) {
        return System.nanoTime() >= deadlineNs;
    }

    /**
     * Use HTTP HEAD method to refresh the channel
     */
    boolean doKeepAlive(Channel ch) {
        final int keepAliveTimeout = 3000; /* ms */
        try {
            final HttpRequest request =
                new DefaultFullHttpRequest(HTTP_1_1, HEAD, "/");

            /*
             * All requests need a HOST header or the LBaaS (nginx) or
             * other server may reject them and close the connection
             */
            request.headers().add(HOST, host);
            FullHttpResponse response = ConcurrentUtil.awaitFuture(
                runRequest(request, ch, keepAliveTimeout));
            /*
             * LBaaS will return a non-200 status but that is expected as the
             * path "/" does not map to the service. This is ok because all that
             * matters is that the connection remain alive.
             */
            String conn = response.headers().get(CONNECTION);
            if (conn == null || !"keep-alive".equalsIgnoreCase(conn)) {
                logFine(logger,
                        "Keepalive HEAD request did not return keep-alive " +
                        "in connection header, is: " + conn);
            }

            return true;
        }  catch (Throwable t) {
            String msg = String.format(
                "Exception sending keepalive on [channel:%s] error:%s",
                ch.id(), t.getMessage());
            logFine(logger, msg);
        }
        /* something went wrong, caller is responsible for disposition */
        return false;
    }
}
