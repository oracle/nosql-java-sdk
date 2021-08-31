/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/*
 * If this code is ever made generic, the proxy information obtained
 * from this config needs to be be abstracted to a generic class.
 */
import oracle.nosql.driver.NoSQLHandleConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

/**
 * Netty HTTP client. Initialization process:
 *
 * 1. create event loop for handling connections and requests. Assign it
 * a number of threads.
 * 2. bootstrap a client, setting the event loop group, socket options, and
 * remote address.
 * 3. create a ChannelPoolHandler instance to handle callback events from
 * a ChannelPool. The pool is used to allow the client to create new
 * connections on demand if one is busy. HTTP/1.1 doesn't allow concurrent
 * use of a single connection. This handler must be prepared to initialize
 * each new connection on creation.
 * 4. create a ChannelPool instance used to acquire and release channels for
 * use by requests.
 *
 * Using the client to send request and get a synchronous response. The
 * request must be an instance of HttpRequest:
 *
 * 1. Get a Channel.
 *   Channel channel = client.getChannel(timeoutMs);
 * 2. Create a ResponseHandler to handle a response.
 *   ResponseHandler rhandler = new ResponseHandler(client, logger, channel);
 * Note that the ResponseHandler will release the Channel.
 * 3. Call runRequest to send the request.
 *   client.runRequest(request, rhandler, channel);
 * 4. For synchronous calls, wait for a response:
 *  rhandler.await(timeoutMs);
 * If there was a problem with the send or receive this call will throw a
 * Throwable with the relevant information. If successful the response
 * information can be extracted from the ResponseHandler.
 * ResponseHandler instances must be closed using the close() method. This
 * releases resources associated with the request/response dialog such as the
 * channel and the HttpResponse itself.
 *
 * TODO: asynchronous handler
 */
public class HttpClient {

    static final int DEFAULT_MAX_PENDING = 3;
    static final int DEFAULT_MAX_CONTENT_LENGTH = 32 * 1024 * 1024; // 32MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    /*
     * timeout for acquiring a Netty channel in ms. If exceeded a new
     * connection is created
     */
    static final int ACQUIRE_TIMEOUT = 5;

    static final AttributeKey<RequestState> STATE_KEY =
        AttributeKey.<RequestState>valueOf("rqstate");

    private final FixedChannelPool pool;
    private final HttpClientChannelPoolHandler poolHandler;

    private final int maxContentLength;
    private final int maxChunkSize;

    private final String host;
    private final int port;
    private final String name;

    /*
     * Non-null if using SSL
     */
    private final SslContext sslCtx;

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
     * Creates a new HttpClient class capable of sending Netty HttpRequest
     * instances and receiving replies. This is a concurrent, asynchronous
     * interface capable of sending and receiving on multiple HTTP channels
     * at the same time.
     *
     * @param host the hostname for the HTTP server
     * @param port the port for the HTTP server
     * @param numThreads the number of async threads to use for Netty
     * notifications. If 0, a default value is used (2).
     * @param connectionPoolSize the max number of HTTP connections to use
     * for concurrent requests. If 0, a default value is used (2)
     * @param sslCtx if non-null, SSL context to use for connections.
     * @param name A name to use in logging messages for this client.
     * @param logger A logger to use for logging messages.
     */
    public HttpClient(String host,
                      int port,
                      int numThreads,
                      int connectionPoolSize,
                      int poolMaxPending,
                      SslContext sslCtx,
                      String name,
                      Logger logger) {
        this(host, port, numThreads, connectionPoolSize, poolMaxPending,
            DEFAULT_MAX_CONTENT_LENGTH, DEFAULT_MAX_CHUNK_SIZE,
            sslCtx, name, logger);
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
     * notifications. If 0, a default value is used (2).
     * @param connectionPoolSize the max number of HTTP connections to use
     * for concurrent requests. If 0, a default value is used (2).
     * @param maxContentLength maximum size in bytes of requests/responses.
     * If 0, a default value is used (32MB).
     * @param maxChunkSize maximum size in bytes of chunked response messages.
     * If 0, a default value is used (64KB).
     * @param sslCtx if non-null, SSL context to use for connections.
     * @param name A name to use in logging messages for this client.
     * @param logger A logger to use for logging messages.
     */
    public HttpClient(String host,
                      int port,
                      int numThreads,
                      int connectionPoolSize,
                      int poolMaxPending,
                      int maxContentLength,
                      int maxChunkSize,
                      SslContext sslCtx,
                      String name,
                      Logger logger) {

        this.logger = logger;
        this.sslCtx = sslCtx;
        this.host = host;
        this.port = port;
        this.name = name;

        poolMaxPending = (poolMaxPending == 0 ?
                              DEFAULT_MAX_PENDING : poolMaxPending);

        this.maxContentLength = (maxContentLength == 0 ?
                              DEFAULT_MAX_CONTENT_LENGTH : maxContentLength);
        this.maxChunkSize = (maxChunkSize == 0 ?
                              DEFAULT_MAX_CHUNK_SIZE : maxChunkSize);

        int cores = Runtime.getRuntime().availableProcessors();

        if (numThreads == 0) {
            numThreads = cores*2;
        }
        if (connectionPoolSize == 0) {
            connectionPoolSize = cores*2;
        }
        workerGroup = new NioEventLoopGroup(12); //numThreads);
        Bootstrap b = new Bootstrap();

        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.remoteAddress(host, port);

        poolHandler =
            new HttpClientChannelPoolHandler(this);

        pool = new FixedChannelPool(
            b,
            poolHandler, /* pool handler */
            poolHandler, /* health checker */
            /*
             * if a channel cannot be acquired in ACQUIRE_TIMEOUT ms, create
             * a new one, even if the pool is full. Consider exposing this
             * behavior as user-facing configuration
             */
            FixedChannelPool.AcquireTimeoutAction.NEW,
            ACQUIRE_TIMEOUT,
            12, //connectionPoolSize,
            poolMaxPending,
            true); /* do health check on release */
    }

    SslContext getSslContext() {
        return sslCtx;
    }

    int getPort() {
        return port;
    }

    String getHost() {
        return host;
    }

    String getName() {
        return name;
    }

    Logger getLogger() {
        return logger;
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
        return pool.acquiredChannelCount();
    }

    /**
     * Cleanly shut down the client.
     */
    public void shutdown() {
        pool.close();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }

    public Channel getChannel(int timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {

        while (true) {
            Future<Channel> fut = pool.acquire();
            Channel retChan = fut.get(timeoutMs, TimeUnit.MILLISECONDS);
            /*
             * Ensure that the channel is in good shape
             */
            if (fut.isSuccess() && retChan.isActive()) {
                return retChan;
            }
            logInfo(logger,
                    "HttpClient " + name + ", acquired an inactive channel, " +
                    "releasing it and retrying, reason: " + fut.cause());
            releaseChannel(retChan);
        }
    }

    public void releaseChannel(Channel channel) {
        /*
         * If channel is not healthy/active it will be closed and removed
         * from the pool. Don't wait for completion.
         */
        pool.release(channel);
    }

    /**
     * Sends an HttpRequest, setting up the ResponseHandler as the handler to
     * use for the (asynchronous) response.
     *
     * @param request the request
     * @param handler the response handler
     * @param channel the Channel to use for the request/response
     *
     * @throws IOException if there is a network problem (bad channel). Such
     * exceptions can be retried.
     */
    public void runRequest(HttpRequest request,
                           ResponseHandler handler,
                           Channel channel)

        throws IOException {

        /*
         * If the channel goes bad throw IOE to allow the caller to retry
         */
        if (!channel.isActive()) {
            String msg = "HttpClient " + name + ", runRequest, channel " +
                channel + " is not active: ";
            logWarning(logger, msg);
            throw new IOException(msg);
        }

        RequestState state = new RequestState(handler);
        channel.attr(STATE_KEY).set(state);

        /*
         * Send the request. If the operation fails set the exception
         * on the ResponseHandler where it will be thrown synchronously to
         * users of that object. operationComplete will likely be called in
         * another thread.
         */
        channel.writeAndFlush(request).
            addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            /* handleException logs this exception */
                            handler.handleException("HttpClient: send failed",
                                                    future.cause());
                        }
                    }
                });
    }
}
