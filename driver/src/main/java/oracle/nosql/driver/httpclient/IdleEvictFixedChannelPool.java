/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A FixedChannelPool that comes pre-wired with:
 * 1. Metrics (Total, Acquired, Idle, Pending)
 * 2. Idle Eviction (Close connections unused for X seconds)
 */
public class IdleEvictFixedChannelPool implements ChannelPool {
    private final FixedChannelPool delegate;
    /* Tracks all active connections. Auto-removes them when they close */
    private final ChannelGroup allChannels;
    private final AtomicInteger acquiredCount = new AtomicInteger(0);
    private final AtomicInteger pendingAcquireCount = new AtomicInteger(0);

    /**
     * Create new instance
     * @param bootstrap the {@link Bootstrap} that is used for connections
     * @param handler the {@link ChannelPoolHandler} that will be notified for
     * the different pool actions
     * @param maxConnections the number of maximal active connections,
     * once this is reached new tries to acquire a {@link Channel} will be
     * delayed until a connection is returned to the pool again.
     * @param maxPendingAcquires the maximum number of pending acquires.
     * Once this is exceed acquire tries will be failed.
     * @param idleTimeoutSeconds The duration in seconds that a channel
     * sits unused in the pool before being automatically closed and evicted.
     * If this value is less than or equal to zero, idle channels won't be
     * evicted.
     */
    IdleEvictFixedChannelPool(Bootstrap bootstrap,
                              ChannelPoolHandler handler,
                              int maxConnections,
                              int maxPendingAcquires,
                              int idleTimeoutSeconds) {

        this.allChannels = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);

        /* Wrap the user's handler with our Idle handler */
        ChannelPoolHandler idleHandler =
            new IdleAwarePoolHandler(handler, idleTimeoutSeconds);

        this.delegate = new FixedChannelPool(bootstrap,
                                             idleHandler,
                                             maxConnections,
                                             maxPendingAcquires);
    }

    @Override
    public Future<Channel> acquire() {
        return acquire(ImmediateEventExecutor.INSTANCE.newPromise());
    }

    @Override
    public Future<Channel> acquire(Promise<Channel> promise) {
        pendingAcquireCount.incrementAndGet();

        /* Create an INTERNAL Promise that Netty's pool will complete */
        Promise<Channel> internalPromise =
            ImmediateEventExecutor.INSTANCE.newPromise();

        /* Ask the delegate pool to notify our INTERNAL promise, not the user
         * passed promise
         */
        delegate.acquire(internalPromise);

        internalPromise.addListener(f -> {
            pendingAcquireCount.decrementAndGet();
            if (f.isSuccess()) {
                Channel ch = (Channel) f.getNow();
                acquiredCount.incrementAndGet();
                /* ChannelGroup acts as a Set.
                 * Adding the same channel multiple times (reuse) is
                 * safe/ignored.
                 */
                allChannels.add(ch);
                promise.setSuccess(ch);
            } else {
                /* Propagate failure */
                promise.setFailure(f.cause());
            }
        });
        return promise;
    }

    @Override
    public Future<Void> release(Channel channel) {
        return release(channel, ImmediateEventExecutor.INSTANCE.newPromise());
    }

    @Override
    public Future<Void> release(Channel channel, Promise<Void> promise) {
        acquiredCount.decrementAndGet();
        return delegate.release(channel, promise);
    }

    @Override
    public void close() {
        delegate.close();
        allChannels.close();
    }

    public PoolStats getStats() {
        int total = allChannels.size();
        int acquired = acquiredCount.get();
        int pending = pendingAcquireCount.get();
        /* Safety: Idle cannot be negative (handles race conditions) */
        int idle = Math.max(0, total - acquired);
        return new PoolStats(total, acquired, idle, pending);
    }

    /* Simple DTO for the stats
     * Pool stats are not atomic consistent view of the pool.
     */
    public static class PoolStats {
        public final int total;
        public final int acquired;
        public final int idle;
        public final int pending;

        public PoolStats(int total, int acquired, int idle, int pending) {
            this.total = total;
            this.acquired = acquired;
            this.idle = idle;
            this.pending = pending;
        }

        @Override
        public String toString() {
            return String.format("PoolStats{Total=%d, " +
                                "Acquired=%d, Idle=%d, Pending=%d}",
                                total, acquired, idle, pending);
        }
    }

    /**
     * A {@link ChannelPoolHandler} which detects idle channels and closes them
     * when the channel is idle for configured time.
     * This class wraps another {@link ChannelPoolHandler}
     * typically {@link HttpClientChannelPoolHandler}.
     */
    public static class IdleAwarePoolHandler implements ChannelPoolHandler {
        private static final String IDLE_HANDLER = "idleWatcher";
        private static final String EVICT_HANDLER = "idleEvictor";
        private final int idleSeconds;
        private final ChannelPoolHandler delegate;

        IdleAwarePoolHandler(ChannelPoolHandler delegate,
                             int idleSeconds) {
            this.delegate = delegate;
            this.idleSeconds = idleSeconds;
        }

        @Override
        public void channelCreated(Channel ch) throws Exception {
            assert ch.eventLoop().inEventLoop();
            delegate.channelCreated(ch);
        }

        @Override
        public void channelAcquired(Channel ch) throws Exception {
            /* channel is in use now, remove Idle handler */
            assert ch.eventLoop().inEventLoop();
            if (idleSeconds > 0) {
                if (ch.pipeline().get(IDLE_HANDLER) != null) {
                    ch.pipeline().remove(IDLE_HANDLER);
                }
                if (ch.pipeline().get(EVICT_HANDLER) != null) {
                    ch.pipeline().remove(EVICT_HANDLER);
                }
            }
            delegate.channelAcquired(ch);
        }

        @Override
        public void channelReleased(Channel ch) throws Exception {
            /* Channel returned to pool: START the idle timer */
            if (idleSeconds > 0) {
                if (ch.pipeline().get(IDLE_HANDLER) == null) {
                    ch.pipeline().addFirst(IDLE_HANDLER,
                        new IdleStateHandler(0, 0,
                                             idleSeconds, TimeUnit.SECONDS));
                        ch.pipeline().addAfter(IDLE_HANDLER, EVICT_HANDLER,
                                new EvictionHandler());
                }
            }
            delegate.channelReleased(ch);
        }

        /* class to handle the idle timeout event */
        static class EvictionHandler extends ChannelInboundHandlerAdapter {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                if (evt instanceof IdleStateEvent) {
                    /* Close the channel.
                     * The FixedChannelPool will detect this and remove it from
                       the queue.
                     * The MonitoredChannelPool's ChannelGroup will detect this
                       and decrement 'Total'.
                     */
                    ctx.close();
                } else {
                    super.userEventTriggered(ctx, evt);
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                super.exceptionCaught(ctx, cause);
            }
        }
    }
}
