/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.values.MapValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Flow.Publisher} that wraps a
 * {@link oracle.nosql.driver.NoSQLHandleAsync#query(QueryRequest)} to
 * iteratively request pages and stream all items from a paginated query.
 *
 * <p>Key properties:
 * <ul>
 * <li>Single subscription </li>
 * <li>Backpressure-aware: items are emitted only up to the downstream demand.</li>
 * <li>At most one remote call in-flight at a time.</li>
 * <li>Buffers up to a single page beyond current demand.</li>
 * <li>Terminates with {@code onComplete()} after all pages are consumed and
 *  the buffer is drained, or with {@code onError(Throwable)} if the remote
 *  call fails.</li>
 * <li>Per Reactive Streams rule 3.9, non-positive requests trigger
 * {@code onError(IllegalArgumentException)}.</li>
 * </ul>
 *
 * <p>Thread-safety:
 * <ul>
 * <li>This publisher supports concurrent calls to
 * {@link Flow.Subscription#request(long)} and
 * {@link Flow.Subscription#cancel()}.</li>
 * <li>Signals to the subscriber are serialized
 * (no concurrent {@code onNext} calls).</li>
 * </ul>
 * @implSpec The publisher:
 * <ul>
 * <li>Starts fetching only when there is outstanding demand and no buffered items.</li>
 * <li>Never emits more than requested items; any overage from a page is buffered.</li>
 * <li>Checks completion both while emitting and after emission even if demand becomes zero,
 * ensuring {@code onComplete()} can be delivered without additional demand.</li>
 * <li>Attempts to cancel any in-flight future on {@code cancel()}.</li>
 * </ul>
 */
final class QueryPublisher implements Flow.Publisher<List<MapValue>> {

    final QueryPaginatorResult queryPaginatorResult;
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    public QueryPublisher(QueryPaginatorResult queryPaginatorResult) {
        this.queryPaginatorResult = queryPaginatorResult;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super List<MapValue>> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber should not be null");
        /* only allow one subscriber */
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                }
                @Override
                public void cancel() {
                }
            });
            subscriber.onError(new IllegalStateException("already subscribed"));
            return;
        }

        QuerySubscription subscription =
            new QuerySubscription(queryPaginatorResult, subscriber);
        try {
            subscriber.onSubscribe(subscription);
        } catch (Throwable t) {
            IllegalStateException err =
                new IllegalStateException(subscriber +
                    " violated the Reactive Streams rule 2.13 by throwing an" +
                    " exception from onSubscribe.", t);
            subscription.signalError(err);
        }
    }
}
