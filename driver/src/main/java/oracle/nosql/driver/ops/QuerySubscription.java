/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.values.MapValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Subscription that manages backpressure, buffering, and page fetching for a
 * single subscriber.
 * <p>All subscriber signals are serialized. At most one page-fetch is in flight
 * at any time.</p>
 */
class QuerySubscription implements Flow.Subscription {
    private static final Logger logger =
        Logger.getLogger(QuerySubscription.class.getName());
    private final QueryPaginatorResult queryPaginatorResult;
    private final QueryRequest queryRequest;
    private final NoSQLHandleAsync handle;
    /* Backpressure and state */
    private final AtomicLong demand;
    private final AtomicInteger wip;
    private final AtomicBoolean started;
    private final Flow.Subscriber<? super List<MapValue>> subscriber;
    private volatile boolean cancelled;

    /* Buffer stores at most a single page beyond demand */
    private final Queue<List<MapValue>> queue;
    private final int limit;
    private final List<MapValue> partialBatch;

    /* set to true when request.isDone() is true after a fetch */
    private volatile boolean done;
    private volatile Throwable error; /* set on failure */

    /* Ensure only one in-flight query at a time */
    private volatile CompletableFuture<QueryResult> inFlight;
    private final AtomicBoolean requestClosed = new AtomicBoolean();

    public QuerySubscription(QueryPaginatorResult queryPaginatorResult,
            Flow.Subscriber<? super List<MapValue>> subscriber) {
        this.queryPaginatorResult = queryPaginatorResult;
        this.subscriber = subscriber;
        this.queryRequest = queryPaginatorResult.queryRequest;
        this.handle = queryPaginatorResult.handle;

        demand = new AtomicLong();
        wip = new AtomicInteger();
        started = new AtomicBoolean();
        queue = new ConcurrentLinkedQueue<>();
        limit = queryRequest.getLimit();
        partialBatch = (limit > 0) ? new ArrayList<>(limit) : null;
    }

    @Override
    public void request(long n) {
        if (cancelled) {
            return;
        }
        if (n <= 0) {
            /* Spec: negative or zero demand is illegal -> onError and cancel */
            onErrorOnce(new IllegalArgumentException(subscriber +
                " violated the Reactive Streams rule 3.9 by requesting a " +
                "non-positive number of elements."));
            signalError(error);
            return;
        }
        Backpressure.addCap(demand, n);
        drain();
    }

    @Override
    public void cancel() {
        cancelled = true;
        closeQueryRequest();
    }

    /* Core loop: serialize all emission/fetch transitions */
    private void drain() {
        if (wip.getAndIncrement() != 0) {
            return;
        }
        int missed = 1;
        while (true) {
            if (cancelled) {
                queue.clear();
                return;
            }
            long r = demand.get();
            long e = 0L;

            /* Emit from buffer up to r(demand) */
            while (e != r) {
                if (cancelled) {
                    queue.clear();
                    return;
                }
                boolean d = done;
                List<MapValue> v = queue.poll();
                boolean empty = (v == null);
                if (d && empty) {
                    Throwable ex = error;
                    if (ex != null) {
                        signalError(ex);
                    } else {
                        signalComplete();
                    }
                    return;
                }
                if (empty) {
                    break;
                }
                signaNext(v);
                e++;
            }

            if (e != 0L) {
                Backpressure.produced(demand, e);
            }

            /* terminal check must also run when r == 0 (no demand) */
            if (done && queue.isEmpty()) {
                Throwable ex = error;
                if (ex != null) {
                    signalError(ex);
                } else {
                    signalComplete();
                }
                return;
            }

            // If buffer is empty, not done, and there is outstanding demand, fetch next page
            if (!cancelled && queue.isEmpty() && !done && demand.get() > 0) {
                maybeFetchNext();
            }

            int w = wip.get();
            if (missed == w) {
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            } else {
                missed = w;
            }
        }
    }

    private void maybeFetchNext() {
        /* Only start a new fetch if no in-flight */
        if (inFlight != null) {
            return;
        }

        CompletableFuture<QueryResult> f;
        try {
            f = handle.query(queryRequest);
        } catch (Throwable ex) {
            onErrorOnce(ex);
            done = true;
            /* Ensure terminal signal is delivered */
            drain();
            return;
        }
        inFlight = f;
        f.whenCompleteAsync((res, err) -> {
            inFlight = null;
            if (cancelled)
                return;
            if (err != null) {
                onErrorOnce(err);
                done = true;
            } else {
                try {
                    setStats(res);
                    List<MapValue> page = res.getResults();
                    if (!page.isEmpty()) {
                        bufferPage(page, false);
                    }
                    if (queryRequest.isDone()) {
                        if (limit > 0) {
                            flushPartialBatch();
                        }
                        done = true;
                    }
                } catch (Throwable ex) {
                    onErrorOnce(ex);
                    done = true;
                }
            }
            drain();
        });
    }

    private void onErrorOnce(Throwable ex) {
        if (error == null) {
            error = ex;
        }
    }

    private void bufferPage(List<MapValue> page, boolean terminal) {
        if (limit == 0) {
            queue.add(page);
            return;
        }

        for (MapValue value : page) {
            partialBatch.add(value);
            if (partialBatch.size() == limit) {
                queue.add(new ArrayList<>(partialBatch));
                partialBatch.clear();
            }
        }

        if (terminal && !partialBatch.isEmpty()) {
            queue.add(new ArrayList<>(partialBatch));
            partialBatch.clear();
        }
    }

    private void flushPartialBatch() {
        if (limit > 0 && !partialBatch.isEmpty()) {
            queue.add(new ArrayList<>(partialBatch));
            partialBatch.clear();
        }
    }
    private void closeQueryRequest() {
        if (requestClosed.compareAndSet(false, true)) {
            try  {
                queryRequest.close();
            } catch (Throwable ignored) {

            }
        }
    }

    void signaNext(List<MapValue> v) {
        try {
            subscriber.onNext(v);
        } catch (Throwable ex) {
            /* downstream threw error. Make sure that we are
             * cancelled, since we cannot do anything else since the
            `* Subscriber` is faulty.
             */
            cancel();
            onErrorOnce(ex);
            logger.log(Level.WARNING,
                subscriber +
                " violated the Reactive Streams rule 2.13 by " +
                "throwing an exception from onNext." +
                ex);
        }
    }

    void signalError(Throwable ex) {
        if (cancelled)
            return;
        cancelled = true; // ensure terminal
        try {
            closeQueryRequest();
            subscriber.onError(ex);
        } catch (Throwable t) {
            logger.log(Level.WARNING, subscriber +
                " violated the Reactive Streams rule 2.13 by " +
                "throwing an exception from onError.", t);
        }
    }

    void signalComplete() {
        if (cancelled)
            return;
        cancelled = true; // ensure terminal
        try {
            closeQueryRequest();
            subscriber.onComplete();
        } catch (Throwable t) {
            logger.log(Level.WARNING, subscriber +
                " violated the Reactive Streams rule 2.13 by " +
                "throwing an exception from onComplete.", t);
        }
    }

    /* Update query metrics on result object */
    private void setStats(QueryResult internalResult) {
        queryPaginatorResult.readKB.addAndGet(internalResult.getReadKB());
        queryPaginatorResult.readUnits.addAndGet(internalResult.getReadUnits());
        queryPaginatorResult.writeKB.addAndGet(internalResult.getWriteKB());
        queryPaginatorResult.writeUnits.addAndGet(
            internalResult.getWriteUnits());
        queryPaginatorResult.setRateLimitDelayedMs(
            queryPaginatorResult.getRateLimitDelayedMs() +
                internalResult.getRateLimitDelayedMs());
        queryPaginatorResult.setReadKB(
            queryPaginatorResult.getReadKB() + internalResult.getReadKB());
        queryPaginatorResult.setReadUnits(
            queryPaginatorResult.getReadUnits() + internalResult.getReadUnits());
        queryPaginatorResult.setWriteKB(
            queryPaginatorResult.getWriteKB() + internalResult.getWriteKB());
        if (internalResult.getRetryStats() != null) {
            if (queryPaginatorResult.getRetryStats() == null) {
                queryPaginatorResult.setRetryStats(new RetryStats());
            }
            queryPaginatorResult.getRetryStats().addStats(
                internalResult.getRetryStats());
        }
    }

    /* Small utility to handle requested arithmetic safely */
    static final class Backpressure {
        static void addCap(AtomicLong requested, long n) {
            for (; ; ) {
                long r = requested.get();
                long u = r + n;
                if (u < 0L) { // overflow -> cap
                    u = Long.MAX_VALUE;
                }
                if (requested.compareAndSet(r, u)) {
                    return;
                }
            }
        }

        static void produced(AtomicLong requested, long n) {
            for (; ; ) {
                long r = requested.get();
                long u = r - n;
                if (u < 0L) {
                    u = 0L;
                }
                if (requested.compareAndSet(r, u)) {
                    return;
                }
            }
        }
    }
}
