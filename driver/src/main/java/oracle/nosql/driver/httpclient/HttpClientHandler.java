/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.IOException;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 *
 */
@Sharable
public class HttpClientHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger;

    HttpClientHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final RequestState state =
            ctx.channel().attr(HttpClient.STATE_KEY).get();

        /*
         * TODO/think about:
         *  o both sync and async operation
         *  o cancelled requests
         *  o redirects
         */

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse fhr = (FullHttpResponse) msg;
            state.setResponse(fhr);

            /*
             * Notify the response handler
             */
            state.getHandler().receive(state);

            return;
        }
        logWarning(logger,
                   "HttpClientHandler, response not FullHttpResponse: " +
                   msg.getClass());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        final RequestState state =
            ctx.channel().attr(HttpClient.STATE_KEY).get();
        if (state != null) {
            /* handleException logs */
            state.getHandler().handleException("HttpClientHandler read failed",
                                               cause);
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final RequestState state =
            ctx.channel().attr(HttpClient.STATE_KEY).get();
        /* handleException logs */
        if (state != null) {
            String msg = "Channel is inactive: " + ctx.channel();
            state.getHandler().handleException(msg, new IOException(msg));
        }
        /* should the context be closed? */
        ctx.close();
    }
}
