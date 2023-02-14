/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;

import java.io.Closeable;
import java.net.ProtocolException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

/**
 * This class allows for asynchronous or synchronous request operation.
 * An instance is passed when sending a request. The caller can handle the
 * response asynchronously by overriding the responseReceived() method, or
 * synchronously by using the default implementation and waiting for the
 * response.
 * <p>
 * Instances of this class must be closed using close().
 *
 * TODO: examples of both sync and async usage
 */
public class ResponseHandler implements Closeable {

    private HttpResponseStatus status;
    private HttpHeaders headers;
    private ByteBuf content;
    private RequestState state;
    private final HttpClient httpClient;
    private final Channel channel;
    private final String requestId;

    /* logger may be null */
    private final Logger logger;

    /* this is set if there is an exception in send or receive */
    private Throwable cause;

    /* OK to retry: affects logic when there are specific protocol errors */
    private final boolean allowRetry;

    /*
     * the latch counts down when the response is received. It's only needed
     * in synchronous mode
     */
    private final CountDownLatch latch;

    public ResponseHandler(final HttpClient httpClient,
                           final Logger logger,
                           final Channel channel) {
        this(httpClient, logger, channel, null, false);
    }

    public ResponseHandler(final HttpClient httpClient,
                           final Logger logger,
                           final Channel channel,
                           final String requestId,
                           boolean allowRetry) {
        this.httpClient = httpClient;
        this.logger = logger;
        this.channel = channel;
        this.requestId = requestId;
        this.allowRetry = allowRetry;

        /*
         * TODO: this won't be needed for an async client
         */
        latch = new CountDownLatch(1);
    }

    /**
     * An exception occurred. Set cause and count down the latch to wake
     * up any waiters. This is synchronized because the call may come from
     * a different thread.
     */
    public void handleException(String msg, Throwable th) {

        synchronized(this) {
            this.cause = th;
            latch.countDown();
        }
        logFine(logger, msg + ", cause: " + th);
    }

    /**
     * The full response has been received. Users can override this method
     * to do full async operation. Synchronous users will wait for the latch
     * and get the response objects from this class.
     */
    public void responseReceived(HttpResponseStatus rStatus,
                                 HttpHeaders rHeaders,
                                 ByteBuf rContent) {
        status = rStatus;
        headers = rHeaders;
        content = rContent;
    }

    /**
     * Wait for the latch to count down. This can happen on a successful
     * receive operation or an exception that occurs during send or receive.
     */
    public boolean await(int milliSeconds) throws Throwable {

        boolean ret = !latch.await(milliSeconds, TimeUnit.MILLISECONDS);

        synchronized(this) {
            if (cause != null) {
                throw cause;
            }
        }
        return ret;
    }

    /**
     * Gets the status, or null if the operation has not yet completed
     */
    public HttpResponseStatus getStatus() {
        return status;
    }

    /**
     * Gets the headers, or null if the operation has not yet completed
     */
    public HttpHeaders getHeaders() {
        return headers;
    }

    /**
     * Gets the content, or null if the operation has not yet completed
     */
    public ByteBuf getContent() {
        return content;
    }

    /**
     * Gets the Throwable if an exception has occurred during send or
     * receive
     */
    public Throwable getCause() {
        return cause;
    }

    /**
     * Internal close that does not release the channel. This is used
     * by keepalive HEAD requests
     */
    void releaseResponse() {
        if (state != null) {
            if (state.getResponse() != null) {
                ReferenceCountUtil.release(state.getResponse());
            }
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            httpClient.releaseChannel(channel);
        }

        /*
         * Release the response
         */
        releaseResponse();
    }

    /*
     * TODO: error responses with and without status
     */

    /*
     * Internal receive that calls the public method and counts down the latch.
     * Use try/finally in case there is a throw in the receive.
     */
    void receive(RequestState requestState) {
        /*
         * Check the request id in response's header, discards this response
         * if it is not for the request.
         */
        if (requestId != null) {
            String resReqId = requestState.getHeaders().get(REQUEST_ID_HEADER);
            if (resReqId == null || !resReqId.equals(requestId)) {
                logFine(logger,
                        "Expected response for request " + requestId +
                        ", but got response for request " + resReqId +
                        ": discarding response");
                if (resReqId == null) {
                    logFine(logger, "Headers for discarded response: " +
                            requestState.getHeaders());
                    if (this.allowRetry) {
                        this.cause = new ProtocolException(
                                "Received invalid response with no requestId");
                        latch.countDown();
                    }
                }
                if (requestState.getResponse() != null) {
                    ReferenceCountUtil.release(requestState.getResponse());
                }
                return;
            }
        }

        /*
         * We got a valid message: don't accept any more for this handler.
         * This logic may change if we enable full async and allow multiple
         * messages to be processed by the same channel for the same client.
         * This clears the response handler from this channel so that any
         * additional messages on this channel will be properly discarded.
         */
        channel.attr(HttpClient.STATE_KEY).set(null);

        state = requestState;
        try {
            responseReceived(state.getStatus(),
                             state.getHeaders(),
                             state.getBuf());
        } finally {
            latch.countDown();
        }
    }
}
