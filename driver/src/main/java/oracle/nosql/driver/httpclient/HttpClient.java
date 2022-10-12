/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.driver.util.HttpConstants.CONNECTION;
import static oracle.nosql.driver.util.LogUtil.isFineEnabled;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
/*
 * If this code is ever made generic, the proxy information obtained
 * from this config needs to be be abstracted to a generic class.
 */
import oracle.nosql.driver.NoSQLHandleConfig;

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

    static final int DEFAULT_MAX_CONTENT_LENGTH = 32 * 1024 * 1024; // 32MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 3000;
    static final int DEFAULT_MIN_POOL_SIZE = 2; // min pool size

    static final AttributeKey<RequestState> STATE_KEY =
        AttributeKey.<RequestState>valueOf("rqstate");

    //private final FixedChannelPool pool;
    private final ConnectionPool pool;
    private final HttpClientChannelPoolHandler poolHandler;

    private final int maxContentLength;
    private final int maxChunkSize;

    private final String host;
    private final int port;
    private final String name;
    private final List<String> httpProtocols;
    private final String httpFallbackProtocol;

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
     * @param httpProtocols A list of preferred http protocols (H2 and Http1.1)
     */
    public static HttpClient createMinimalClient(String host,
                                                 int port,
                                                 SslContext sslCtx,
                                                 int handshakeTimeoutMs,
                                                 String name,
                                                 List<String> httpProtocols,
                                                 Logger logger) {
        return new HttpClient(host,
                              port,
                              1, /* nThreads */
                              0, /* pool min */
                              0, /* pool inactivity period */
                              true, /* minimal client */
                              DEFAULT_MAX_CONTENT_LENGTH,
                              DEFAULT_MAX_CHUNK_SIZE,
                              sslCtx, handshakeTimeoutMs, name, httpProtocols, logger);
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
     * @param httpProtocols A list of preferred http protocols (H2 and Http1.1)
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
                      List<String> httpProtocols,
                      Logger logger) {

        this(host, port, numThreads, connectionPoolMinSize,
             inactivityPeriodSeconds, false /* not minimal */,
             maxContentLength, maxChunkSize, sslCtx, handshakeTimeoutMs, name, httpProtocols, logger);
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
                       List<String> httpProtocols,
                       Logger logger) {

        this.logger = logger;
        this.sslCtx = sslCtx;
        this.host = host;
        this.port = port;
        this.name = name;

        this.httpProtocols = httpProtocols.size() > 0 ?
                httpProtocols :
                new ArrayList<>(Arrays.asList(ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1));

        this.httpFallbackProtocol = this.httpProtocols.get(this.httpProtocols.size() - 1);

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

    int getPort() {
        return port;
    }

    String getHost() {
        return host;
    }

    String getName() {
        return name;
    }

    List<String> getHttpProtocols() {
        return httpProtocols;
    }

    public String getHttpFallbackProtocol() {
        return httpFallbackProtocol;
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
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }

    public Channel getChannel(int timeoutMs)
        throws InterruptedException, ExecutionException, TimeoutException {

        long startMs = System.currentTimeMillis();
        long now = startMs;
        int retries = 0;

        while (true) {
            long msDiff = now - startMs;

            /* retry loop with at most (retryInterval) ms timeouts */
            long thisTimeoutMs = (timeoutMs - msDiff);
            if (thisTimeoutMs <= 0) {
                String msg = "Timed out after " + msDiff +
                             "ms (" + retries + " retries) trying " +
                             "to acquire channel";
                logInfo(logger, "HttpClient " + name + " " + msg);
                throw new TimeoutException(msg);
            }
            if (thisTimeoutMs > acquireRetryIntervalMs) {
                thisTimeoutMs = acquireRetryIntervalMs;
            }
            Future<Channel> fut = pool.acquire();
            Channel retChan = null;
            try {
                retChan = fut.get(thisTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                if (retries == 0) {
                    logInfo(logger, "Timed out after " +
                            (System.currentTimeMillis() - startMs) +
                            "ms trying to acquire channel: retrying");
                }
                /* fall through */
            }

            /*
             * Ensure that the channel is in good shape. retChan is null
             * on a timeout exception from above; that path will retry.
             */
            if (retChan != null) {
                if (fut.isSuccess() && retChan.isActive()) {
                    /*
                     * Clear out any previous state. The channel should not
                     * have any state associated with it, but this code is here
                     * just in case it does.
                     */
                    if (retChan.attr(STATE_KEY).get() != null) {
                        if (isFineEnabled(logger)) {
                            logFine(logger,
                                    "HttpClient acquired a channel with " +
                                    "a still-active state: clearing.");
                        }
                        retChan.attr(STATE_KEY).set(null);
                    }
                    return retChan;
                }
                logFine(logger,
                        "HttpClient " + name + ", acquired an inactive " +
                        "channel, releasing it and retrying, reason: " +
                        fut.cause());
                releaseChannel(retChan);
            }
            /* reset "now" and increment retries */
            now = System.currentTimeMillis();
            retries++;
        }
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

    /**
     * Use HTTP HEAD method to refresh the channel
     */
    boolean doKeepAlive(Channel ch) {
        final int keepAliveTimeout = 3000; /* ms */
        ResponseHandler responseHandler =
            new ResponseHandler(this, logger, ch);
        try {
            final HttpRequest request =
                new DefaultFullHttpRequest(HTTP_1_1, HEAD, "/");

            /*
             * All requests need a HOST header or the LBaaS (nginx) or
             * other server may reject them and close the connection
             */
            request.headers().add(HOST, host);
            runRequest(request, responseHandler, ch);
            boolean isTimeout = responseHandler.await(keepAliveTimeout);
            if (isTimeout) {
                logFine(logger,
                        "Timeout on keepalive HEAD request on channel " + ch);
                return false;
            }
            /*
             * LBaaS will return a non-200 status but that is expected as the
             * path "/" does not map to the service. This is ok because all that
             * matters is that the connection remain alive.
             */
            String conn = responseHandler.getHeaders().get(CONNECTION);
            if (conn == null || !"keep-alive".equalsIgnoreCase(conn)) {
                logFine(logger,
                        "Keepalive HEAD request did not return keep-alive " +
                        "in connection header, is: " + conn);
            }

            return true;
        } catch (Throwable t) {
            logFine(logger, "Exception sending HTTP HEAD: " + t);
        } finally {
            responseHandler.releaseResponse();
        }
        /* something went wrong, caller is responsible for disposition */
        return false;
    }
}
