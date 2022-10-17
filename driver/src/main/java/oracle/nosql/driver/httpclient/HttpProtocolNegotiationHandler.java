/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logFine;

import java.net.SocketAddress;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.util.internal.RecyclableArrayList;
import oracle.nosql.driver.util.HttpConstants;

/**
 * Handle TLS protocol negotiation result, either Http1.1 or H2
 *
 * The channel initialization process:
 * 1. Channel acquired from {@link ConnectionPool} after channel is active.
 * 2. SSL negotiation started, pipeline is not ready.
 * 3. {@link HttpProtocolNegotiationHandler} holds all {@link HttpMessage} while waiting for the negotiation result.
 * 4. Negotiation finished, {@link HttpProtocolNegotiationHandler} changes the pipeline according to the protocol selected.
 * 5. {@link HttpProtocolNegotiationHandler} removes itself from the pipeline. Writes any buffered {@link HttpMessage} to the channel.
 */
public class HttpProtocolNegotiationHandler extends ApplicationProtocolNegotiationHandler implements ChannelOutboundHandler {
    private static final String HTTP_HANDLER_NAME = "http-client-handler";

    private final Logger logger;
    private final RecyclableArrayList bufferedMessages = RecyclableArrayList.newInstance();
    private final HttpClientHandler handler;
    private final int maxChunkSize;
    private final int maxContentLength;

    public HttpProtocolNegotiationHandler(String fallbackProtocol, HttpClientHandler handler, int maxChunkSize, int maxContentLength, Logger logger) {
        super(fallbackProtocol);

        this.logger = logger;
        this.handler = handler;
        this.maxChunkSize = maxChunkSize;
        this.maxContentLength = maxContentLength;
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
        if (HttpConstants.HTTP_2.equals(protocol)) {
            HttpUtil.configureHttp2(ctx.pipeline(), this.maxContentLength);
        } else if (HttpConstants.HTTP_1_1.equals(protocol)) {
            HttpUtil.configureHttp1(ctx.pipeline(), this.maxChunkSize, this.maxContentLength);
        } else {
            throw new IllegalStateException("unknown http protocol: " + protocol);
        }
        logFine(this.logger, "HTTP protocol selected: " + protocol);
        ctx.pipeline().addLast(HTTP_HANDLER_NAME, handler);
    }

    /*
     * User can write requests right after the channel is active, while protocol
     * negotiation is still in progress. At this stage the pipeline is not ready
     * to write http requests, so we must hold them here.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object o, ChannelPromise channelPromise) throws Exception {
        if (o instanceof HttpMessage) {
            HttpUtil.Pair<Object, ChannelPromise> p = HttpUtil.Pair.of(o, channelPromise);
            this.bufferedMessages.add(p);
            return;
        }

        // let non-http message to pass, so the HTTP2 preface and settings frame can be sent
        ctx.write(o, channelPromise);
    }

    /*
     * Protocol negotiation finish, handler removed, the pipeline is
     * ready to handle http messages. Write previously buffered http messages.
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        HttpUtil.writeBufferedMessages(ctx.channel(), this.bufferedMessages);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress socketAddress, ChannelPromise channelPromise) {
        ctx.bind(socketAddress, channelPromise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) throws Exception {
        ctx.connect(socketAddress, socketAddress1, channelPromise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise channelPromise) {
        ctx.disconnect(channelPromise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise channelPromise) throws Exception {
        ctx.close(channelPromise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise channelPromise) {
        ctx.deregister(channelPromise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        ctx.flush();
    }

}

