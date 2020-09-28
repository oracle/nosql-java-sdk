/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;

/**
 * This is an instance of Netty's ChannelPoolHandler used to initialize
 * channels as they are created, acquired, and released from a pool of Channels
 * configured for an HTTP client.
 */
@Sharable
public class HttpClientChannelPoolHandler implements ChannelPoolHandler,
                                                     ChannelHealthChecker {

    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";
    private static final String HTTP_HANDLER_NAME = "http-response-handler";

    private final HttpClient client;

    /*
     * Debugging -- use these to ensure proper acquire/release calls.
     * TODO: keep these or not?
     */
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicInteger createCount = new AtomicInteger(0);

    /**
     * Creates an instance of this object
     *
     * @param client the HttpClient instance. It is used to get configuration
     * options.
     */
    HttpClientChannelPoolHandler(HttpClient client) {
        this.client = client;
    }

    @Override
    public void channelAcquired(Channel ch) {
        //logFine(client.getLogger(), "Channel acquired: " + ch);
        count.incrementAndGet();
    }

    /**
     * Initialize a channel with handlers that:
     * 1 -- handle and HTTP
     * 2 -- handle chunked HTTP requests implicitly, only calling channelRead
     * with FullHttpResponse.
     * 3 -- the response handler itself
     *
     * TODO: HttpContentCompressor?
     */
    @Override
    public void channelCreated(Channel ch) {
        logFine(client.getLogger(),
                "HttpClient " + client.getName() + ", channel created: " + ch);
        ChannelPipeline p = ch.pipeline();
        if (client.getSslContext() != null) {
            /* Enable hostname verification */
            final SslHandler sslHandler = client.getSslContext().newHandler(
                ch.alloc(), client.getHost(), client.getPort());
            final SSLEngine sslEngine = sslHandler.engine();
            final SSLParameters sslParameters = sslEngine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
            p.addLast(sslHandler);
        }
        p.addLast(CODEC_HANDLER_NAME, new HttpClientCodec
                              (4096, // initial line
                               8192, // header size
                               client.getMaxChunkSize()));
        p.addLast(AGG_HANDLER_NAME, new HttpObjectAggregator(
                                  client.getMaxContentLength()));
        p.addLast(HTTP_HANDLER_NAME,
                              new HttpClientHandler(client.getLogger()));

        if (client.getProxyHost() != null) {
            p.addFirst("proxyServer", new HttpProxyHandler(
                           new InetSocketAddress(client.getProxyHost(),
                                                 client.getProxyPort()),
                           client.getProxyUsername(),
                           client.getProxyPassword()));
        }
        count.incrementAndGet();
        createCount.incrementAndGet();
    }

    @Override
    public void channelReleased(Channel ch) {
        /*
         * no need to log this -- it happens on every release. What would be
         * nice is to log when a channel is destroyed, but that doesn't seem
         * possible
         */
        //logFine(client.getLogger(), "Channel release: " + ch);
        count.decrementAndGet();
    }

    /*
     * Maybe remove the count-based debugging
     */
    void close() {
        int chCount = count.get();
        logInfo(client.getLogger(),
                "HttpClient " + client.getName() +
                ", close pool handler, create count, ref count: "
                + createCount.get() + ", " + chCount);
        if (chCount > 0) {
            logInfo(client.getLogger(),
                    "HttpClient " + client.getName() +
                    ", possible channel leak, count is non-zero: " + chCount);
        }
    }

    /**
     * Returns the number of channels that are in active use. Note that this is
     * less than or equal to the number of channels currently in the pool.
     */
    int getCount() {
        return count.get();
    }

    /**
     * Returns the total number of channels that were created over the lifetime
     * of the pool. This could be greater than the max number of connections
     * configured for the pool if unhealthy connections are replaced with new
     * healthy ones.
     */
    int getCreateCount() {
        return createCount.get();
    }

    /**
     * Implements ChannelHealthChecker. This is the same as Netty's
     * ChannelHealthChecker.ACTIVE but logs if the channel isn't active.
     */
    @Override
    public Future<Boolean> isHealthy(Channel channel) {
        boolean val = channel.isActive();

        if (!val) {
            logInfo(client.getLogger(),
                    "HttpClient " + client.getName() +
                    ", channel inactive in health check: " + channel);
        }
        EventLoop loop = channel.eventLoop();
        return val? loop.newSucceededFuture(Boolean.TRUE) :
            loop.newSucceededFuture(Boolean.FALSE);
    }
}
