/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdleEvictFixedChannelPoolTest {
    private EventLoopGroup group;
    private Channel serverChannel;
    private LocalAddress address;

    @Before
    public void setUp() throws InterruptedException {
        group = new NioEventLoopGroup();
        address = new LocalAddress("test-port");

        /* Start a fake Local Server so the pool can connect */
        ServerBootstrap sb = new ServerBootstrap()
            .group(group)
            .channel(LocalServerChannel.class)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
                }
            });
        serverChannel = sb.bind(address).sync().channel();
    }

    @After
    public void tearDown() {
        serverChannel.close();
        group.shutdownGracefully();
    }

    @Test
    public void testMetricsAndReuse() throws Exception {
        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(LocalChannel.class)
                .remoteAddress(address);

        /* A dummy user handler (noop) */
        ChannelPoolHandler noopHandler = new ChannelPoolHandler() {
            public void channelReleased(Channel ch) {}
            public void channelAcquired(Channel ch) {}
            public void channelCreated(Channel ch) {}
        };

        IdleEvictFixedChannelPool pool =
            new IdleEvictFixedChannelPool(bootstrap,
                                          noopHandler,
                                          2,
                                          5,
                                          5);

        /* CHECK 1: Initial */
        assertStats(pool, 0, 0, 0, 0);

        /* CHECK 2: Acquire */
        Channel ch1 = pool.acquire().sync().getNow();
        /* Total:1, Acquired:1, Idle:0 */
        assertStats(pool, 1, 1, 0, 0);

        /* CHECK 3: Release (Idle Logic should kick in) */
        pool.release(ch1).sync();
        /* Total:1, Acquired:0, Idle:1 */
        assertStats(pool, 1, 0, 1, 0);

        /* CHECK 4: Reuse */
        Channel ch2 = pool.acquire().sync().getNow();
        /* Should be the SAME channel object (reused) */
        assertEquals(ch1.id(), ch2.id());
        /* Stats: Total:1, Acquired:1, Idle:0 */
        assertStats(pool, 1, 1, 0, 0);

        /* Cleanup */
        pool.close();
    }

    @Test
    public void testIdleEvictionInPool() throws InterruptedException {
        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(LocalChannel.class)
                .remoteAddress(address);

        /* A dummy user handler (noop) */
        ChannelPoolHandler noopHandler = new ChannelPoolHandler() {
            public void channelReleased(Channel ch) {}
            public void channelAcquired(Channel ch) {}
            public void channelCreated(Channel ch) {}
        };

        try(IdleEvictFixedChannelPool pool =
            new IdleEvictFixedChannelPool(bootstrap,
                    noopHandler,
                    2,
                    5,
                    60)) {

            /* 1. Acquire a channel */
            Channel ch = pool.acquire().sync().getNow();

            /* 2. Release it back to the pool (This starts the Idle Timer) */
            pool.release(ch).sync();

            /* Verify it's currently Idle */
            assertTrue(ch.isOpen());
            assertEquals(1, pool.getStats().idle);

            /* 3. SIMULATE the timeout Event
             * Instead of waiting 1 minute, we force the event specifically on
             * this channel
             */
            ch.pipeline().
                fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);


            /* 4. Assert Eviction - The channel should be closed */
            Thread.sleep(100);
            assertFalse("Channel should be closed after idle event",
                        ch.isOpen());

            /* The metrics should update (Total drops to 0) */
            assertEquals("Total count should drop to 0",
                         0, pool.getStats().total);
            assertEquals("Idle count should drop to 0",
                         0, pool.getStats().idle);
        }
    }

    @Test
    public void testNoEvictionWhenAcquired() throws InterruptedException {
        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(LocalChannel.class)
                .remoteAddress(address);

        /* A dummy user handler (noop) */
        ChannelPoolHandler noopHandler = new ChannelPoolHandler() {
            public void channelReleased(Channel ch) {}
            public void channelAcquired(Channel ch) {}
            public void channelCreated(Channel ch) {}
        };

        IdleEvictFixedChannelPool pool =
            new IdleEvictFixedChannelPool(bootstrap,
                                          noopHandler,
                                          2,
                                          5,
                                          60);

        /* 1. Acquire a channel */
        Channel ch = pool.acquire().sync().getNow();

        /* 2. Release it (Timer starts) */
        pool.release(ch).sync();

        /* 3. Re-acquire it immediately (Timer should STOP) */
        Channel reusedCh = pool.acquire().sync().getNow();
        assertEquals( "Should reuse the same channel", ch, reusedCh);

        /* 4. Simulate the Timeout Event
         * Even though we fire the event, the handler should not have been
         * REMOVED
         */
        ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);

        /* 5. Assert Safety */
        assertTrue("Active channel should NOT be closed", ch.isOpen());
        assertEquals("Channel should remain acquired",
                     1, pool.getStats().acquired);
        pool.release(ch).sync();
        pool.close();
        assertStats(pool, 0, 0, 0, 0);
    }

    @Test
    public void testMaxConnectionsAndPendingQueue() throws InterruptedException {
        int numberOfRequests = 5;
        int maxConnections = 2;

        /* Thread-safe list to hold the channels we successfully acquire */
        List<Channel> heldChannels =
            Collections.synchronizedList(new ArrayList<>());

        /* Latch to wait ONLY for the allowed connections to succeed */
        CountDownLatch acquiredLatch = new CountDownLatch(maxConnections);

        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(LocalChannel.class)
                .remoteAddress(address);

        /* A dummy user handler (noop) */
        ChannelPoolHandler noopHandler = new ChannelPoolHandler() {
            public void channelReleased(Channel ch) {}
            public void channelAcquired(Channel ch) {}
            public void channelCreated(Channel ch) {}
        };

        IdleEvictFixedChannelPool pool =
            new IdleEvictFixedChannelPool(bootstrap,
                                          noopHandler,
                                          maxConnections,
                                          numberOfRequests + 1,
                                          60);
        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        /* PHASE 1: Bombard the pool */
        for (int i = 0; i < numberOfRequests; i++) {
            threadPool.submit(() -> {
                Future<Channel> future = pool.acquire();
                future.addListener(f -> {
                    if (f.isSuccess()) {
                        heldChannels.add((Channel) f.getNow());
                        acquiredLatch.countDown();
                    }
                });
            });
        }
        /* Wait for the pool to fill up (Max maxConnections) */
        boolean success = acquiredLatch.await(5, TimeUnit.SECONDS);
        if (!success) {
            throw new RuntimeException("Timeout waiting for initial connections");
        }

        /* Give a tiny buffer for metrics to settle */
        Thread.sleep(50);

        /* PHASE 2: Assert Saturation */
        IdleEvictFixedChannelPool.PoolStats stats = pool.getStats();
        assertEquals("Total should be capped at max",
                     maxConnections, stats.total);
        assertEquals("Acquired should be capped at max",
                     maxConnections, stats.acquired);
        assertEquals("Excess requests should be pending",
                     numberOfRequests - maxConnections, stats.pending);

        /* PHASE 3: Drain the Queue
         * Now we manually release the channels we were holding.
         * This should trigger the Pending requests to proceed.
         */

        /* We need a new latch to verify the REMAINING 3 requests finish
         * (But we can't easily attach listeners now, so we just check stats)
         */
        for (Channel ch : heldChannels) {
            pool.release(ch);
        }

        /* Wait a moment for the pending queue to drain */
        Thread.sleep(200);
        stats = pool.getStats();

        /* Expect: Pending should be one now. */
        assertEquals("Pending queue should have 1", 1, stats.pending);

        threadPool.shutdown();
        pool.close();
    }

    private void assertStats(IdleEvictFixedChannelPool pool, int t,
                             int a, int i, int p) {
        IdleEvictFixedChannelPool.PoolStats s = pool.getStats();
        assertEquals("Total", t, s.total);
        assertEquals("Acquired", a, s.acquired);
        assertEquals("Idle", i, s.idle);
        assertEquals("Pending", p, s.pending);
    }
}
