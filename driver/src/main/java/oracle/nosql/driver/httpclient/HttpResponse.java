/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClientResponse;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class HttpResponse {
    private final HttpClientResponse reactorNettyResponse;
    private final Connection reactorNettyConnection;

    private final Logger logger =
            Logger.getLogger(HttpResponse.class.getName());
    /*
     0 - not subscribed
     1 - subscribed,
     2 - cancelled via connector, (before subscribe)
     3 - error (before subscribe)
     */
    private final AtomicReference<SubscriptionState> state =
            new AtomicReference<>(SubscriptionState.NOT_SUBSCRIBED);

    HttpResponse(HttpClientResponse reactorNettyResponse, Connection reactorNettyConnection) {
        this.reactorNettyResponse = reactorNettyResponse;
        this.reactorNettyConnection =  reactorNettyConnection;
    } 

    public int getStatusCode() {
        return reactorNettyResponse.status().code();
    }

    public HttpResponseStatus getStatus() {
        return reactorNettyResponse.status();
    }

    public HttpHeaders getHeaders() {
        return reactorNettyResponse.responseHeaders();
    }

    public Mono<String> getBodyAsString() {
        return reactorNettyConnection.inbound().receive()
            .aggregate()
            .asString()
            .doOnSubscribe(this::updateSubscriptionState);
    }

    public String getBodyAsStringSync() {
        return getBodyAsString().block();
    }
    public Mono<ByteBuf> getBody() {
        return reactorNettyConnection.inbound().receive()
            .aggregate()
            .doOnSubscribe(this::updateSubscriptionState);
    }


    /*
     * Update the subscription state to SUBSCRIBED when body is susbscribed
     */
    void updateSubscriptionState(Subscription subscription) {
        if (this.state.compareAndSet(SubscriptionState.NOT_SUBSCRIBED,
                SubscriptionState.SUBSCRIBED)) {
            return;
        }
        /* https://github.com/reactor/reactor-netty/issues/503
         * FluxReceive rejects multiple subscribers but only if the second
         * subscribes while first one is still active.
         * Once a subscriber completes and the receiver field is cleared,
         * a second subscriber will not be rejected and will not receive further
         * signals.
         */
        if (this.state.get() == SubscriptionState.CANCELLED) {
            throw new IllegalStateException(
                    "The client response body has been released already due to cancellation.");
        }
    }

    /*
     *  Drain unsubscribed body on cancel or error
     */
    void releaseUnSubscribedResponse(SubscriptionState state) {
        if (this.state.compareAndSet(SubscriptionState.NOT_SUBSCRIBED, state)) {
            logger.fine("Releasing body for not yet subscribed response");
            reactorNettyConnection.inbound().receive()
                    .doOnNext(byteBuf -> {})
                    .subscribe(byteBuf -> {}, ex -> {});
        }
    }

     enum SubscriptionState {
        // 0 - not subscribed, 1 - subscribed, 2 - cancelled via connector (before subscribe)
        // 3 - error occurred before subscribe
        //
        NOT_SUBSCRIBED, // 0 - not subscribed,
        SUBSCRIBED, // 1 - subscribed
        CANCELLED, // 2 - cancelled before subscribe
        ERROR // 3 - error before subscribe
    }
}
