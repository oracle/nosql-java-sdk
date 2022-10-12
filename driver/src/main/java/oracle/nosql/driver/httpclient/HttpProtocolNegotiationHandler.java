/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static io.netty.handler.logging.LogLevel.DEBUG;
import static oracle.nosql.driver.util.LogUtil.logFine;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.util.internal.RecyclableArrayList;

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
    private static final Http2FrameLogger frameLogger = new Http2FrameLogger(DEBUG, HttpProtocolNegotiationHandler.class);

    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";
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

    private void writeBufferedMessages(ChannelHandlerContext ctx) {
        if (!this.bufferedMessages.isEmpty()) {
            for(int i = 0; i < this.bufferedMessages.size(); ++i) {
                Pair<Object, ChannelPromise> p = (Pair<Object, ChannelPromise>)this.bufferedMessages.get(i);
                ctx.channel().write(p.first, p.second);
            }

            this.bufferedMessages.clear();
        }
        this.bufferedMessages.recycle();
    }

    private void configureHttp1(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();

        p.addLast(CODEC_HANDLER_NAME,
                new HttpClientCodec(4096, // initial line
                        8192, // header size
                        maxChunkSize)); // chunksize
        p.addLast(AGG_HANDLER_NAME,
                new HttpObjectAggregator(maxContentLength));
    }

    private void configureHttp2(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();

        Http2Connection connection = new DefaultHttp2Connection(false);
        HttpToHttp2ConnectionHandler connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
                .frameListener(new DelegatingDecompressorFrameListener(
                        connection,
                        new InboundHttp2ToHttpAdapterBuilder(connection)
                                .maxContentLength(this.maxContentLength)
                                .propagateSettings(false)
                                .build()))
                .frameLogger(frameLogger)
                .connection(connection)
                .build();

        p.addLast(connectionHandler);
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            configureHttp2(ctx);
        } else if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
            configureHttp1(ctx);
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
            Pair<Object, ChannelPromise> p = Pair.of(o, channelPromise);
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
        this.writeBufferedMessages(ctx);
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

    private static class Pair<A, B> {

        public final A first;
        public final B second;

        public Pair(A fst, B snd) {
            this.first = fst;
            this.second = snd;
        }

        public String toString() {
            return "Pair[" + first + "," + second + "]";
        }

        public boolean equals(Object other) {
            if (other instanceof Pair<?, ?>) {
                Pair<?,?> pair = (Pair<?,?>) other;
                return Objects.equals(first, pair.first) &&
                        Objects.equals(second, pair.second);
            }
            return false;
        }

        public int hashCode() {
            if (first == null)
                return (second == null) ? 0 : second.hashCode() + 1;
            else if (second == null)
                return first.hashCode() + 2;
            else
                return first.hashCode() * 17 + second.hashCode();
        }

        public static <A, B> Pair<A, B> of(A a, B b) {
            return new Pair<>(a, b);
        }
    }
}

