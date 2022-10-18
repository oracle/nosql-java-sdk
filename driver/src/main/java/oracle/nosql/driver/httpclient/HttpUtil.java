/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static io.netty.handler.logging.LogLevel.DEBUG;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.RecyclableArrayList;

public class HttpUtil {
    private static final Http2FrameLogger frameLogger = new Http2FrameLogger(DEBUG, HttpUtil.class);

    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";

    public static void removeHttpObjectAggregator(ChannelPipeline p) {
        p.remove(AGG_HANDLER_NAME);
    }

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

        p.addLast(CODEC_HANDLER_NAME, sourceCodec);
        p.addLast(upgradeHandler);
        p.addLast(AGG_HANDLER_NAME, new HttpObjectAggregator(maxContentLength));
        p.addLast(new UpgradeRequestHandler());
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

            ch.flush();
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
     * A handler that triggers the H2C upgrade to HTTP/2 by sending an initial HTTP1.1 request.
     *
     */
    private static final class UpgradeRequestHandler extends ChannelDuplexHandler {
        private final RecyclableArrayList bufferedMessages = RecyclableArrayList.newInstance();
        private UpgradeEvent upgradeResult = null;

        /**
         * In channelActive event, we send a probe request "HEAD / Http1.1 upgrade: h2c" to proxy.
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            DefaultFullHttpRequest upgradeRequest =
                    new DefaultFullHttpRequest(HTTP_1_1, HEAD, "/", Unpooled.EMPTY_BUFFER);

            // Set HOST header as the remote peer may require it.
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            String hostString = remote.getHostString();
            if (hostString == null) {
                hostString = remote.getAddress().getHostAddress();
            }
            upgradeRequest.headers().set(HOST, hostString + ':' + remote.getPort());

            ctx.writeAndFlush(upgradeRequest);

            ctx.fireChannelActive();
        }

        /*
         * Upgrading, we temporarily hold all user requests.
         */
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof HttpMessage) {
                Pair<Object, ChannelPromise> p = Pair.of(msg, promise);
                this.bufferedMessages.add(p);
                return;
            }

            ctx.write(msg, promise);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            // HttpClientUpgradeHandler received the first response from proxy
            // Based on the response, it triggers UpgradeEvent, either SUCCEFULL or REJECTED.
            if (evt instanceof UpgradeEvent) {
                // This can also be UpgradeEvent.UPGRADE_ISSUED
                // But it will be overwritten when the first response arrive
                upgradeResult = (UpgradeEvent) evt;
                // If upgrade is SUCCESSFUL, at this point the Http2ConnectionHandler is installed
                // We don't need Aggregator anymore, remove it.
                if (upgradeResult == UpgradeEvent.UPGRADE_SUCCESSFUL) {
                    HttpUtil.removeHttpObjectAggregator(ctx.pipeline());
                }
            }
            if (evt instanceof UpgradeFinishedEvent &&
                    upgradeResult == UpgradeEvent.UPGRADE_SUCCESSFUL) {
                // The HttpClientUpgradeHandler is removed from pipeline
                // Upgrade is SUCCESSFUL
                // The pipeline is now configured for H2
                // Remove this handler and flush the buffered messages
                ctx.pipeline().remove(this);
            }
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            // When upgrade is rejected (Old version proxy, does not support Http2)
            // the proxy sends a "400 Bad Request" response, drop it here.
            // Also remove this handler and flush the buffered messages
            if (msg instanceof FullHttpResponse &&
                    upgradeResult == UpgradeEvent.UPGRADE_REJECTED) {
                FullHttpResponse rep = (FullHttpResponse) msg;
                if (BAD_REQUEST.equals(rep.status())) {
                    // Just drop the first "400" response, remove this handler
                    ReferenceCountUtil.release(msg);
                    ctx.pipeline().remove(this);
                    return;
                }
            }
            ctx.fireChannelRead(msg);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            super.handlerRemoved(ctx);
            writeBufferedMessages(ctx.channel(), this.bufferedMessages);
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
