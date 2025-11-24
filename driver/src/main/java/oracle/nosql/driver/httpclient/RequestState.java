/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An instance of this class is created when a request is sent and is used to
 * collect response state. The instance is attached to a Channel's attribute
 * map, which means that this will work for HTTP/1.1 where channels are not
 * multiplexed, but will need to change for HTTP/2.
 *
 * This class is not thread-safe but is used in a safe, single-threaded manner
 * mapped 1:1 with a channel associated with a single HTTP request/response
 * cycle.
 *
 * At this time this object does not aggregate chunks of content into a single
 * buffer. It is expected that this is done using an HttpContentAggregator in
 * the pipeline and is only called with a FullHttpResponse. If aggregation is
 * desired here it can be added using a CompositeByteBuf and calls to add
 * content incrementally.
 */
class RequestState {

    private final ResponseHandler handler;
    private FullHttpResponse response;

    RequestState(ResponseHandler handler) {
        this.handler = handler;
    }

    ResponseHandler getHandler() {
        return handler;
    }

    HttpResponseStatus getStatus() {
        if (response != null) {
            return response.status();
        }
        return null;
    }

    HttpHeaders getHeaders() {
        if (response != null) {
            return response.headers();
        }
        return null;
    }

    int getContentSize() {
        ByteBuf buf = getBuf();
        if (buf != null) {
            return buf.readableBytes();
        }
        return -1;
    }

    ByteBuf getBuf() {
        if (response != null) {
            return response.content();
        }
        return null;
    }

    void setResponse(FullHttpResponse response) {
        this.response = response;
    }

    FullHttpResponse getResponse() {
        return response;
    }
}
