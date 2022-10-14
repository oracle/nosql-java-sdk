/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static io.netty.handler.logging.LogLevel.DEBUG;

import java.net.SocketAddress;
import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.util.internal.RecyclableArrayList;

public class HttpUtil {
    private static final Http2FrameLogger frameLogger = new Http2FrameLogger(DEBUG, HttpProtocolNegotiationHandler.class);

    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";

    public static void configureHttp1(ChannelPipeline p, int maxChunkSize, int maxContentLength) {
        p.addLast(CODEC_HANDLER_NAME,
                new HttpClientCodec(4096, // initial line
                        8192, // header size
                        maxChunkSize)); // chunksize
        p.addLast(AGG_HANDLER_NAME,
                new HttpObjectAggregator(maxContentLength));
    }

    public static void configureHttp2(ChannelPipeline p, int maxContentLength) {
        p.addLast(createHttp2ConnectionHandler(maxContentLength));
    }

    public static void configureH2C(ChannelPipeline p, int maxChunkSize, int maxContentLength) {
        HttpClientCodec sourceCodec = new HttpClientCodec(4096, 8192, maxChunkSize);
        Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(createHttp2ConnectionHandler(maxContentLength));
        HttpClientUpgradeHandler upgradeHandler = new UpgradeHandler(sourceCodec, upgradeCodec, maxContentLength);
        p.addLast(sourceCodec,
                upgradeHandler,
                new UpgradeRequestHandler(maxContentLength));
    }

    private static Http2ConnectionHandler createHttp2ConnectionHandler(int maxContentLength) {
        Http2Connection connection = new DefaultHttp2Connection(false);
        HttpToHttp2ConnectionHandler connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
                .frameListener(new DelegatingDecompressorFrameListener(
                        connection,
                        new InboundHttp2ToHttpAdapterBuilder(connection)
                                .maxContentLength(maxContentLength)
                                .propagateSettings(false)
                                .build()))
                .frameLogger(frameLogger)
                .connection(connection)
                .build();
        return connectionHandler;
    }

    public static void writeBufferedMessages(Channel ch, RecyclableArrayList bufferedMessages) {
        if (!bufferedMessages.isEmpty()) {
            for(int i = 0; i < bufferedMessages.size(); ++i) {
                Pair<Object, ChannelPromise> p = (Pair<Object, ChannelPromise>)bufferedMessages.get(i);
                ch.write(p.first, p.second);
            }

            bufferedMessages.clear();
        }
        bufferedMessages.recycle();
    }

    private static final class UpgradeHandler extends HttpClientUpgradeHandler {
        public UpgradeHandler(SourceCodec sourceCodec, UpgradeCodec upgradeCodec, int maxContentLength) {
            super(sourceCodec, upgradeCodec, maxContentLength);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            super.handlerRemoved(ctx);
            ctx.fireUserEventTriggered(UpgradeFinishedEvent.INSTANCE);
        }
    }

    /**
     * A handler that holds HttpMessages (Except the first one)
     * So the upgrade handler can have time to finish clear text protocol upgrade
     */
    private static final class UpgradeRequestHandler extends ChannelDuplexHandler {
        private final int maxContentLength;
        private final RecyclableArrayList bufferedMessages = RecyclableArrayList.newInstance();
        private boolean upgrading = false;

        public UpgradeRequestHandler(int maxContentLength) {
            this.maxContentLength = maxContentLength;
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            super.handlerRemoved(ctx);
            writeBufferedMessages(ctx.channel(), this.bufferedMessages);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof HttpClientUpgradeHandler.UpgradeEvent) {
                HttpClientUpgradeHandler.UpgradeEvent reg = (HttpClientUpgradeHandler.UpgradeEvent) evt;
                if (reg == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_REJECTED) {
                    ctx.pipeline().addAfter(ctx.name(), AGG_HANDLER_NAME, new HttpObjectAggregator(maxContentLength));
                }
            }
            if (evt instanceof UpgradeFinishedEvent) {
                // Upgrade Finished (both SUCCESSFUL or REJECTED)
                // The HttpClientUpgradeHandler is removed from pipeline
                // The pipeline is now configured, and ready
                // Remove this handler and flush the buffered messages
                ctx.pipeline().remove(this);
            }
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof HttpMessage) {
                // Only let the very first HttpMessage pass through
                // The H2C upgrade handler modifies the first message,
                // and tries to negotiate a new protocol with the server
                // This process takes one round trip
                if (upgrading) {
                    Pair<Object, ChannelPromise> p = Pair.of(msg, promise);
                    this.bufferedMessages.add(p);
                    return;
                }
                upgrading = true;
            }

            ctx.write(msg, promise);
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            ctx.bind(localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            ctx.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.deregister(promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            ctx.read();
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }
    }

    public static final class UpgradeFinishedEvent {
        private static final UpgradeFinishedEvent INSTANCE = new UpgradeFinishedEvent();

        private UpgradeFinishedEvent() {
        }
    }

    public static class Pair<A, B> {

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
            if (other instanceof Pair<?,?>) {
                Pair<?, ?> pair = (Pair<?, ?>) other;
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
