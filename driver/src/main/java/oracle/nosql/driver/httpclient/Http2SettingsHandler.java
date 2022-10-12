package oracle.nosql.driver.httpclient;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.util.internal.RecyclableArrayList;

import java.util.concurrent.TimeUnit;

public class Http2SettingsHandler extends SimpleChannelInboundHandler<Http2Settings> {
    private final ChannelPromise promise;
    RecyclableArrayList bufferedMessages;
    ChannelHandlerContext ctx;

    public Http2SettingsHandler(ChannelHandlerContext ctx, RecyclableArrayList bufferedMessages) {
        this.ctx = ctx;
        this.promise = ctx.newPromise();
        this.bufferedMessages = bufferedMessages;
    }

    public void awaitSettings(long timeout, TimeUnit unit) throws Exception {
        if (!promise.awaitUninterruptibly(timeout, unit)) {
            throw new IllegalStateException("Timed out waiting for settings");
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Http2Settings http2Settings) throws Exception {
        promise.setSuccess();
        fireBufferedMessages();
        channelHandlerContext.pipeline().remove(this);
    }

    private void fireBufferedMessages() {
        if (!this.bufferedMessages.isEmpty()) {
            for(int i = 0; i < this.bufferedMessages.size(); ++i) {
                Pair<Object, ChannelPromise> p = (Pair<Object, ChannelPromise>)this.bufferedMessages.get(i);
                this.ctx.channel().write(p.first, p.second);
            }

            this.bufferedMessages.clear();
        }
        this.bufferedMessages.recycle();
    }
}
