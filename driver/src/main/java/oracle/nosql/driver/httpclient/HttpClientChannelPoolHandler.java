/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.Future;
import oracle.nosql.driver.util.HttpConstants;

/**
 * This is an instance of Netty's ChannelPoolHandler used to initialize
 * channels as they are created, acquired, and released from a pool of Channels
 * configured for an HTTP client.
 */
@Sharable
public class HttpClientChannelPoolHandler implements ChannelPoolHandler,
                                                     ChannelHealthChecker {

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

    private void configureSSL(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        /* Enable hostname verification */
        final SslHandler sslHandler = client.getSslContext().newHandler(
                ch.alloc(), client.getHost(), client.getPort());
        final SSLEngine sslEngine = sslHandler.engine();
        final SSLParameters sslParameters = sslEngine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        sslEngine.setSSLParameters(sslParameters);
        sslHandler.setHandshakeTimeoutMillis(client.getHandshakeTimeoutMs());

        p.addLast(sslHandler);
        p.addLast(new ChannelLoggingHandler(client));
        // Handle ALPN protocol negotiation result, and configure the pipeline accordingly
        p.addLast(new HttpProtocolNegotiationHandler(
                client.getHttpFallbackProtocol(), new HttpClientHandler(client.getLogger()),
                client.getMaxChunkSize(), client.getMaxContentLength(), client.getLogger()));
    }

    private void configureClearText(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        HttpClientHandler handler = new HttpClientHandler(client.getLogger());
        boolean useHttp2 = client.getHttpProtocols().contains(HttpConstants.HTTP_2);

        // Only true when HTTP_2 is the only protocol, as if user set:
        // config.setHttpProtocols(HttpConstants.HTTP_2);
        if (useHttp2 &&
            HttpConstants.HTTP_2.equals(client.getHttpFallbackProtocol())) {
            // If choose to use H2 and fallback is also H2
            // Then there is no need to upgrade from Http1.1 to H2C
            // Directly connects with H2 protocol, so called Http2-prior-knowledge
            HttpUtil.configureHttp2(ch.pipeline(), client.getMaxContentLength());
            p.addLast(handler);
            return;
        }

        // Only true when HTTP_1_1 is the only protocol, as if user set:
        // config.setHttpProtocols(HttpConstants.HTTP_1_1);
        if (!useHttp2 &&
            HttpConstants.HTTP_1_1.equals(client.getHttpFallbackProtocol())) {
            HttpUtil.configureHttp1(ch.pipeline(), client.getMaxChunkSize(), client.getMaxContentLength());
            p.addLast(handler);
            return;
        }

        // Only true when both HTTP_2 and HTTP_1_1 are available, the default option:
        // config.setHttpProtocols(HttpConstants.HTTP_2,
        //                         HttpConstants.HTTP_1_1)
        if (useHttp2 &&
            HttpConstants.HTTP_1_1.equals(client.getHttpFallbackProtocol())) {
            HttpUtil.configureH2C(ch.pipeline(), client.getMaxChunkSize(), client.getMaxContentLength());
            p.addLast(handler);
            return;
        }
        throw new IllegalStateException("unknown protocol: " + client.getHttpProtocols());
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
        if (client.getSslContext() != null) {
            configureSSL(ch);
        } else {
            configureClearText(ch);
        }

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

            ch.pipeline().addFirst("proxyServer", proxyHandler);
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
