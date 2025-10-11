/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.http.NoSQLHandleAsyncImpl;
import oracle.nosql.driver.values.MapValue;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publisher for query pagination.
 */
public class QueryPublisher implements Flow.Publisher<MapValue> {

    private final NoSQLHandleAsyncImpl handle;
    private final QueryRequest request;
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    public QueryPublisher(NoSQLHandleAsyncImpl handle, QueryRequest request) {
        this.handle = handle;
        this.request = request;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super MapValue> subscriber) {
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

        subscriber.onSubscribe(new Flow.Subscription() {
            private final AtomicBoolean cancelled = new AtomicBoolean(false);
            private final AtomicLong demand = new AtomicLong(0);
            private int currentIndex = 0;
            private List<MapValue> currentBatch = List.of();
            /* first run triggered? */
            private boolean started = false;

            @Override
            public void request(long n) {
                if (n <= 0 || cancelled.get()) return;
                demand.addAndGet(n);
                fetchNext();
            }

            @Override
            public void cancel() {
                cancelled.set(true);
                /* close the query request */
                request.close();
            }

            private void fetchNext() {
                if (cancelled.get()) return;

                /* If batch exhausted, fetch next Result */
                if (currentIndex >= currentBatch.size()) {
                    if (started && request.isDone()) {
                        /* close the query request */
                        request.close();
                        subscriber.onComplete();
                        return;
                    }
                    started = true;
                    handle.query(request).whenComplete((result, error) -> {
                        if (cancelled.get()) return;
                        if (error != null) {
                            request.close();
                            subscriber.onError(error);
                        } else {
                            currentBatch = result.getResults();
                            currentIndex = 0;
                            fetchNext(); /* continue with new batch */
                        }
                    });
                    return;
                }

                /* Emit items while demand > 0 and we still have rows */
                while (demand.get() > 0
                        && currentIndex < currentBatch.size()
                        && !cancelled.get()) {
                    subscriber.onNext(currentBatch.get(currentIndex++));
                    demand.decrementAndGet();
                }

                // If demand still positive but batch finished, fetch more
                if (demand.get() > 0
                        && currentIndex >= currentBatch.size()
                        && !cancelled.get()) {
                    fetchNext();
                }
            }
        });
    }
}
