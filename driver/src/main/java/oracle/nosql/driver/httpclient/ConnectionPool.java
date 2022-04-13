/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

/**
 * A class to manage and pool Netty Channels (connections). This is used
 * instead of one of Netty's ChannelPool instances to allow better control
 * and tracking of Channels.
 *
 * Configuration:
 *   minSize - actively keep this many alive, even after inactivity, by default
 *     this is the number of cores
 *   inactivityPeriod - remove inactive channels after this many seconds.
 *     If negative, don't ever remove them
 *   Logger
 *
 * Usage
 *  o acquire()
 *  o release()
 *
 * How the pool works
 * Basic Operation
 *  o the pool is a double-ended queue, treated as LIFO. Channels in the queue
 *    are *not* in use. In-use channels are "owned" by the acquiring caller
 *  o Channels are "acquired" from the front end and released there
 *  o acquired channels are *removed* from the queue and only returned upon
 *    release
 *  o if no Channels are in the queue for acquire a new one is created and
 *    placed in the queue on release
 *
 * Keep-alive and minimum size
 *  o if a pool is not a minimal pool a refresh task is created on construction.
 *    This task does this:
 *     o walks the queue from the "end" (least recently used) side, closing
 *       Channels that are beyond their inactivity period
 *     o Channels will only be removed down to the minimum size. If minSize is 0
 *       then all channels may be pruned
 *     o if a keepalive callback is configured as well as a minimum size,
 *       keepalive callbacks are performed on minSize channels if they've
 *       exceeded the keepalive inactivity period (30s). In the cloud service
 *       this prevents the server side from closing inactive channels after
 *       65s (a default that cannot be modified).
 */

class ConnectionPool {

    /* remove channels that have not been used in this many seconds */
    final static int DEFAULT_INACTIVITY_PERIOD_SECS = 30;
    /* run the refresh task this often */
    final static int DEFAULT_REFRESH_PERIOD_SECS = 30;
    /* max ensures that keepalives are done within the service idle itimeout */
    final static int MAX_INACTIVITY_PERIOD_SECS = 30;

    private final Bootstrap bootstrap;
    private final ChannelPoolHandler handler;
    private final int poolMin;
    private final int inactivityPeriodSeconds;
    private final Logger logger;

    private KeepAlive keepAlive;

    /*
     * Double-ended queue of created, but not in-use Channels. In-use
     * Channels are owned by the acquirer.
     *
     * Remove and insert to front to keep channels "hot"
     * and allow them to time out from the end when demand is reduced
     */
    private final ConcurrentLinkedDeque<Channel> queue;

    /*
     * Map of allocated (both in-use and not) Channel stats. All non-closed
     * Channels exist in this map. They are removed only when the Channel is
     * closed.
     */
    private final Map<Channel, ChannelStats> stats;
    private int acquiredChannelCount;

    /**
     * Keepalive callback interface
     */
    @FunctionalInterface
    interface KeepAlive {
        boolean keepAlive(Channel ch);
    }

    /**
     * A pool of Netty Channels. Channels are reused when available and if none
     * are available a new channel is created. There is no limit to how many
     * will be created if requested. Unless a "minimal" pool is created a
     * refresh task is created that will close idle connections down to the
     * minimum, if configured, or 0 if not.
     *
     * @param bootstrap (netty)
     * @param handler the handler, mostly used for event callbacks
     * @param logger
     * @param isMinimalPool set to true if this is a one-time, or minimal time
     *  use. In this case no refresh task is created
     * @param poolMin the minimum size at which the pool should be maintained.
     *  The pool will only close or otherwise release down to this many
     *  channels. It also sends keepalive requests on up to this many channels.
     * @param inactivityPeriodSeconds an internal idle channel timeout. If a
     * refresh task is running channels idle for this long will be closed, down
     * to the minimum (if set). This allows bursty behavior to automatically
     * clean up when channels are no longer required. This is more for on-prem
     * than the cloud service but applies to both.
     */
    ConnectionPool(Bootstrap bootstrap,
                   ChannelPoolHandler handler,
                   Logger logger,
                   boolean isMinimalPool,
                   int poolMin,
                   int inactivityPeriodSeconds) {
        /* clone bootstrap to set handler */
        this.bootstrap = bootstrap.clone();

        /*
         * channel initialization is done in the handler.channelCreated()
         * callback (see HttpClientChannelPoolHandler
         */
        this.bootstrap.handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    assert ch.eventLoop().inEventLoop();
                    handler.channelCreated(ch);
                }
            });

        this.handler = handler;
        this.logger = logger;
        this.poolMin = poolMin;
        /* period can be between 1 and MAX */
        this.inactivityPeriodSeconds =
            inactivityPeriodSeconds == 0 ? DEFAULT_INACTIVITY_PERIOD_SECS :
            Math.min(inactivityPeriodSeconds, MAX_INACTIVITY_PERIOD_SECS);

        queue = new ConcurrentLinkedDeque<Channel>();
        stats = new ConcurrentHashMap<Channel, ChannelStats>();

        /*
         * If not creating a minimal pool run RefreshTask every 30s. A
         * minimal pool is short-lived so don't create the overhead.
         *
         * See below for what RefreshTask does
         */
        if (!isMinimalPool) {
            /*
             * get the EventExecutor for scheduling the task. If inacivity
             * period is short, make the refresh period short as well
             */
            int refreshPeriod = (this.inactivityPeriodSeconds < 0) ?
                DEFAULT_REFRESH_PERIOD_SECS :
                Math.min(DEFAULT_REFRESH_PERIOD_SECS,
                         this.inactivityPeriodSeconds);
            this.bootstrap.config().group().next()
                .scheduleAtFixedRate(new RefreshTask(),
                                     refreshPeriod, refreshPeriod,
                                     TimeUnit.SECONDS);
        }
    }

    /**
     * Sets a keepalive callback
     */
    void setKeepAlive(KeepAlive ka) {
        this.keepAlive = ka;
    }

    /**
     * See below
     */
    final Future<Channel> acquire() {
        return acquire(bootstrap.config().group().next().<Channel>newPromise());
    }

    /**
     * Acquire a Future for an existing or new Channel. If a not-active/healthy
     * channel is found on the queue, close it and retry. This is not a
     * significant time sink in terms of affecting overall latency of this call
     *
     * Acquired channels are removed from the queue and are "owned" by the
     * caller until released, at which time they are put back on the queue.
     */
    final Future<Channel> acquire(final Promise<Channel> promise) {
        try {
            while (true) {
                /* this *removes* the channel from the queue */
                final Channel channel = queue.pollFirst();
                if (channel == null) {
                    /* need a new Channel */
                    Bootstrap bs = bootstrap.clone();
                    ChannelFuture fut = bs.connect();
                    if (fut.isDone()) {
                        notifyOnConnect(fut, promise);
                    } else {
                        fut.addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(
                                    ChannelFuture future) throws Exception {
                                    notifyOnConnect(future, promise);
                                }
                            });
                    }
                    return promise;
                }
                /*
                 * This logic must happen in the event loop
                 */
                EventLoop loop = channel.eventLoop();
                if (loop.inEventLoop()) {
                    if (checkChannel(channel, promise)) {
                        /* bad channel, try again */
                        continue;
                    }
                } else {
                    loop.execute(new Runnable() {
                            @Override
                            public void run() {
                                checkChannel(channel, promise);
                            }
                        });
                }
                break;
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    /**
     * Release a channel. This is not async. The channel is added to the
     * front of the queue. This class implements a LIFO algorithm to ensure
     * that the first, or first few channels on the queue remain active and
     * are not subject to inactivity timeouts from the server side.
     */
    void release(Channel channel) {
        if (!channel.isActive()) {
            logFine(logger,
                    "Inactive channel on release, closing: " + channel);
            removeChannel(channel);
        }
        updateStats(channel, false);
        queue.addFirst(channel);
        try { handler.channelReleased(channel); } catch (Exception e) {}
    }

    /**
     * Closes and removes a channel from the pool entirely. This channel
     * must not exist in the queue at this time
     */
    private void removeChannel(Channel channel) {
        stats.remove(channel);
        channel.close();
    }

    /**
     * close the pool, removing all channels. This leaves the pool
     * available but empty. Nothing (yet) prevents new acquire() calls.
     * This method should only be called when shutting down the NoSQL handle,
     * or for testing purposes.
     */
    void close() {
        logFine(logger, "Closing pool, stats " + getStats());
        /* TODO: do this cleanly */
        validatePool();
        Channel ch = queue.pollFirst();
        while (ch != null) {
            removeChannel(ch);
            ch = queue.pollFirst();
        }
        validatePool();
    }

    /**
     * How many channels have been acquired since this pool was created
     */
    int getAcquiredChannelCount() {
        return acquiredChannelCount;
    }

    private void notifyOnConnect(ChannelFuture future,
                                 Promise<Channel> promise) throws Exception {
        if (future.isSuccess()) {
            Channel channel = future.channel();
            updateStats(channel, true);
            handler.channelAcquired(channel);
            if (!promise.trySuccess(channel)) {
                /* Promise was completed (like cancelled), release channel */
                release(channel);
            }
        } else {
            promise.tryFailure(future.cause());
        }
    }

    private boolean checkChannel(final Channel channel,
                                 final Promise<Channel> promise) {

        /*
         * If channel isn't healthy close it. It's been removed from
         * the queue
         */
        if (!channel.isActive()) {
            logFine(logger,
                    "Inactive channel found, closing: " + channel);
            removeChannel(channel);
            return true;
        }
        try {
            updateStats(channel, true);
            handler.channelAcquired(channel);
        } catch (Exception e) {} /* ignore */
        promise.setSuccess(channel);
        return false;
    }

    /**
     * Returns the total number of channels, acquired and not, in the pool
     */
    int getTotalChannels() {
        return queue.size() + acquiredChannelCount;
    }

    /**
     * Returns the number of created, but not in-use (acquired) Channels
     */
    int getFreeChannels() {
        return queue.size();
    }

    /**
     * Prune channels
     *  1. remove any inactive channels (closed by other side)
     *  2. remove excess channels that have not been used for
     * inactivityPeriod if set
     *  3. keep poolMin channels
     *
     * @return number of channels closed and removed
     */
    int pruneChannels() {
        int pruned = 0;
        long now = System.currentTimeMillis();

        /* remove inactive channels from queue */
        for (Channel ch : queue) {
            if (!ch.isActive()) {
                logFine(logger,
                        "Channel being pruned due to server close: " + ch);
                queue.remove(ch);
                removeChannel(ch);
                pruned++;
            }
        }

        /**
         * If inactivityPeriodSeconds is negative there is nothing to
         * prune
         */
        if (inactivityPeriodSeconds > 0) {
            while (queue.size() > poolMin) {
                Channel ch = queue.pollLast();
                /*
                 * if channel hasn't been acquired for the inactivity
                 * period, remove it
                 */
                ChannelStats cs = stats.get(ch);
                assert cs != null;
                long inactive = (now - cs.getLastAcquired())/1000;
                if (inactive > inactivityPeriodSeconds) {
                    logFine(logger,
                            "Channel being pruned due to inactivity: " + ch);
                    /* remove from stats and close */
                    removeChannel(ch);
                    pruned++;
                    continue;
                }

                /*
                 * we're working from the end of the LIFO queue so if this
                 * channel isn't inactive or timed out the next one closer to
                 * the front of the queue won't be either, so put it back and
                 * quit
                 */
                queue.addLast(ch);
                break;
            }
        }

        validatePool();
        return pruned;
    }

    /**
     * send keepalive messages for poolMin channels if they are
     * not already active and have not been used for at least
     * keepAlivePeriod
     * @param keepAlivePeriod the number of seconds a channel needs to be
     * inactive before a keepAlive is sent
     * @return the number of keepalive messages sent
     */
    int doKeepAlive(int keepAlivePeriod) {
        if (keepAlive == null) {
            return 0;
        }

        /*
         * If poolMin channels are acquired there's nothing to do.
         * This works for poolMin of 0 as well. If HttpClient is null
         * there is no way to do this either.
         */
        int numToSend = poolMin - acquiredChannelCount;
        if (numToSend <= 0) {
            return 0;
        }

        long now = System.currentTimeMillis();

        /*
         * Don't remove a channel from the queue until it's clear that
         * it will be used
         */
        int numSent = 0;
        for (Channel ch : queue) {
            if (!ch.isActive()) {
                continue;
            }
            ChannelStats cs = stats.get(ch);
            /* queue and stats aren't locked, deal with races */
            if (cs == null) {
                continue;
            }
            long inactive = (now - cs.getLastAcquired())/1000;
            if (inactive >= keepAlivePeriod) {
                if (!queue.remove(ch)) {
                    /* race condition - channel got removed, keep looping */
                    continue;
                }
                logFine(logger,
                        "Sending keepalive on channel " + ch + ", stats: " + cs);
                boolean didKeepalive = keepAlive.keepAlive(ch);
                if (!didKeepalive) {
                    logFine(logger,
                            "Keepalive failed on channel " + ch +
                            ", removing from pool");
                    removeChannel(ch);
                    continue;
                }
                cs.acquired(); /* update lastAcquired time */
                numSent++;
                queue.addFirst(ch);
            }
            /*
             * channels that are not used but have been used within the
             * keepAlivePeriod count as being kept alive as they are
             * healthy and won't time out
             */
            if (--numToSend == 0) {
                break;
            }
        }
        validatePool();

        return numSent;
    }

    private void validatePool() {
        /*
         * Some sanity checking. Stats size should include all channels in the
         * pool -- acquired plus not-acquired
         */
        if ((queue.size() + acquiredChannelCount) != stats.size()) {
            logInfo(logger,
                    "Pool count discrepancy: Queue size, acquired count, " +
                    "stats size :" + queue.size() + ", " +
                    acquiredChannelCount + ", " + stats.size());
        }
    }

    /**
     * Update stats, global and per-Channel
     */
    private void updateStats(Channel channel, boolean isAcquire) {
        ChannelStats cstats = stats.get(channel);
        if (cstats == null) {
            cstats = new ChannelStats();
            stats.put(channel, cstats);
        }
        synchronized(this) {
            if (isAcquire) {
                acquiredChannelCount++;
                cstats.acquired();
            } else {
                acquiredChannelCount--;
            }
        }
    }

    void logStats() {
        logFine(logger, getStats());
    }

    /**
     * Returns stats in String form
     * TODO: JSON-format?
     */
    String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("acquiredCount=" + acquiredChannelCount +
                  ", freeChannelCount=" + queue.size() +
                  ", totalChannelCount=" + stats.size());
        sb.append(", [");
        for (Map.Entry<Channel, ChannelStats> entry : stats.entrySet()) {
            sb.append("channel=" + entry.getKey().id() + "[");
            entry.getValue().toStringBuilder(sb);
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    long getLastAcquired(Channel ch) {
        ChannelStats cs = stats.get(ch);
        if (cs != null) {
            return cs.getLastAcquired();
        }
        logFine(logger, "Can't get stats for channel " + ch.id());
        return 0L;
    }

    int getUseCount(Channel ch) {
        ChannelStats cs = stats.get(ch);
        if (cs != null) {
            return cs.getUseCount();
        }
        logFine(logger, "Can't get stats for channel " + ch.id());
        return 0;
    }

    /**
     * An internal class that maintains stats on Channels. Consider exposing
     * it beyond tests.
     */
    class ChannelStats {
        /* when the channel was last acquired -- timestamp */
        private long lastAcquired;
        /* how many times the channel has been used */
        private int useCount;

        void acquired() {
            lastAcquired = System.currentTimeMillis();
            ++useCount;
        }

        long getLastAcquired() {
            return lastAcquired;
        }

        int getUseCount() {
            return useCount;
        }

        void toStringBuilder(StringBuilder sb) {
            sb.append("useCount=" + useCount +
                      ", lastAcquired=" + java.time.Instant.ofEpochMilli(lastAcquired));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toStringBuilder(sb);
            return sb.toString();
        }
    }

    /**
     * A task to do 2 things on a fixed interval (30s):
     * 1. remove inactive channels if configured to do so (this is the
     * default). Only remove them down to the minimum channels configured.
     * 2. send keepalive HTTP requests on minimum channels to ensure they
     * are not closed by the server side due to inactivity. This is the
     * case in the cloud service.
     */
    private class RefreshTask implements Runnable {
        final int keepAlivePeriod = 30; /* seconds */

        @Override
        public final void run() {
            try {
                pruneChannels();
                if (keepAlive != null) {
                    doKeepAlive(keepAlivePeriod);
                }
            } catch (Exception e) {
                logFine(logger, "Exception in RefreshTask: " + e);
            }
        }
    }
}
