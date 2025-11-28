/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.httpclient.HttpClient.STATE_KEY;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.driver.util.LogUtil.isFineEnabled;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;

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
        final CompletableFuture<FullHttpResponse> responseFuture =
            ctx.channel().attr(STATE_KEY).getAndSet(null);

        /*
         * TODO/think about:
         *  o both sync and async operation
         *  o cancelled requests
         *  o redirects
         */

        /* Remove timeout handler upon response arrival */
        if (ctx.pipeline().get(ReadTimeoutHandler.class) != null) {
           ctx.pipeline().remove(ReadTimeoutHandler.class);
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse fhr = (FullHttpResponse) msg;

            if (responseFuture == null) {
                /*
                 * This message came in after the client was done processing
                 * a request in a different thread.
                 * The client may have timed out waiting for this message.
                 * Discard the message by releasing it and not calling receive().
                 */
                if (isFineEnabled(logger)) {
                    String requestId = fhr.headers().get(REQUEST_ID_HEADER);
                    if (requestId == null) {
                        requestId = "(none)";
                    }
                    logFine(logger, "Discarding message with no response " +
                                    "handler. requestId=" + requestId);
                }
                fhr.release();
                return;
            }
            responseFuture.complete(fhr);
            return;
        }
        logWarning(logger,
                   "HttpClientHandler, response not FullHttpResponse: " +
                   msg.getClass());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        final CompletableFuture<FullHttpResponse> responseFuture =
            ctx.channel().attr(STATE_KEY).getAndSet(null);
        if (responseFuture != null) {
            /* handleException logs */
            logFine(logger, "HttpClientHandler read failed, cause: " + cause);
            Throwable err = cause;
            if (err instanceof ReadTimeoutException) {
                err = new TimeoutException("Request timed out while waiting "
                    + "for the response from the server");
            }
            responseFuture.completeExceptionally(err);
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        final CompletableFuture<FullHttpResponse> responseFuture =
            ctx.channel().attr(STATE_KEY).getAndSet(null);
        /* handleException logs */
        if (responseFuture != null && !responseFuture.isDone()) {
            String msg = "Channel is inactive: " + ctx.channel();
            Throwable cause = new IOException(msg);
            logFine(logger, msg + ", cause: " + cause);
            responseFuture.completeExceptionally(cause);
        }
        /* should the context be closed? */
        ctx.close();
    }
}
