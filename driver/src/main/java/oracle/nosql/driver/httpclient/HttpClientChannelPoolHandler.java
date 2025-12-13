/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logFine;

import java.net.InetSocketAddress;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
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

    /**
     * Creates an instance of this object
     *
     * @param client the HttpClient instance. It is used to get configuration
     * options.
     */
    HttpClientChannelPoolHandler(HttpClient client) {
        this.client = client;
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
                "HttpClient " + client.getName() + ", channel created: " + ch
                + ", acquired channel cnt " + client.getAcquiredChannelCount());
        ChannelPipeline p = ch.pipeline();
        if (client.getSslContext() != null) {
            /* Enable hostname verification */
            final SslHandler sslHandler = client.getSslContext().newHandler(
                ch.alloc(), client.getHost(), client.getPort());

            if (client.isEndpointIdentificationEnabled()) {
                final SSLEngine sslEngine = sslHandler.engine();
                final SSLParameters sslParameters = sslEngine.getSSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(sslParameters);
            }
            sslHandler.setHandshakeTimeoutMillis(client.getHandshakeTimeoutMs());

            p.addLast(sslHandler);
            p.addLast(new ChannelLoggingHandler(client));
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
            InetSocketAddress sockAddr =
                new InetSocketAddress(client.getProxyHost(),
                                      client.getProxyPort());
            HttpProxyHandler proxyHandler =
                client.getProxyUsername() == null ?
                new HttpProxyHandler(sockAddr) :
                new HttpProxyHandler(sockAddr,
                                     client.getProxyUsername(),
                                     client.getProxyPassword());

            p.addFirst("proxyServer", proxyHandler);
        }
    }

    @Override
    public void channelAcquired(Channel ch) {
    }

    @Override
    public void channelReleased(Channel ch) {
    }

    /**
     * Implements ChannelHealthChecker. This is the same as Netty's
     * ChannelHealthChecker.ACTIVE but logs if the channel isn't active.
     */
    @Override
    public Future<Boolean> isHealthy(Channel channel) {
        boolean val = channel.isActive();

        if (!val) {
            logFine(client.getLogger(),
                    "HttpClient " + client.getName() +
                    ", channel inactive in health check: " + channel);
        }
        EventLoop loop = channel.eventLoop();
        return val? loop.newSucceededFuture(Boolean.TRUE) :
            loop.newSucceededFuture(Boolean.FALSE);
    }

    private static class ChannelLoggingHandler
        extends ChannelInboundHandlerAdapter {

        private HttpClient client;

        ChannelLoggingHandler(HttpClient client) {
            this.client = client;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            logFine(client.getLogger(),
                    "HttpClient " + client.getName() +
                    ", channel " + ctx.channel() + " connected");
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logFine(client.getLogger(),
                    "HttpClient " + client.getName() +
                    ", channel " + ctx.channel() + " inactive");
            /* Removing a channel from the pool is handled internally.
             * No need for the below call.
             */
            //client.removeChannel(ctx.channel());
            ctx.fireChannelInactive();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx,
                                       Object evt)
            throws Exception {

            if (evt instanceof SslHandshakeCompletionEvent) {
                if (!((SslHandshakeCompletionEvent) evt).isSuccess()) {
                    logFine(client.getLogger(),
                            "HttpClient " + client.getName() +
                            ", channel: " + ctx.channel() +
                            " handshake failed: " + evt);
                }
            }
            ctx.fireUserEventTriggered(evt);
        }
    }
}
