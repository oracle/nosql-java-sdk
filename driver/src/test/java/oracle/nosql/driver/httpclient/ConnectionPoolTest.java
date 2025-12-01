/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;
import java.net.URL;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * Tests for ConnectionPool
 */
public class ConnectionPoolTest {

    private static String endpoint = System.getProperty("test.endpoint");
    private static Logger logger = getLogger();
    private URL serviceURL;
    private EventLoopGroup group;
    private Channel serverChannel;
    private LocalAddress address;

    @Before
    public void beforeTest() throws InterruptedException {
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

    /**
     * This test is excluded from the test profiles and must be run standalone.
     * This is because of the need to use a cloud endpoint for complete
     * testing. See header comment on testCloudTimeout().
     * It can be run explicitly using either the test-onprem or test-cloudsim
     * profile with a -Dtest directive, e.g.:
     *  mvn -Ptest-cloudsim test \
     *    -Dtest=oracle.nosql.driver.httpclient.ConnectionPoolTest \
     *    -DargLine="-Dtest.endpoint=http://localhost:8080 \
     *    -Dtest.cloudendpoint=some_cloud_endpoint"
     */
    @Test
    public void poolTest() throws Exception {
        Assume.assumeTrue(endpoint != null);

        /* serviceURL is used in the test but a handle is not required */
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        serviceURL = config.getServiceURL();

        final int poolSize = 4;
        final int poolMinSize = 1;
        final int poolInactivityPeriod = 1;

        final HttpClient client = new HttpClient(
            serviceURL.getHost(),
            serviceURL.getPort(),
            0, // threads
            poolMinSize,
            poolInactivityPeriod,
            0, // contentLen
            0, // chunkSize
            null, // sslCtx
            0,    // ssl handshake timeout
            "Pool Test",
            logger);

        ConnectionPool pool = client.getConnectionPool();

        pool.setKeepAlive(new ConnectionPool.KeepAlive() {
                    @Override
                    public boolean keepAlive(Channel ch) {
                        return client.doKeepAlive(ch);
                    }
                });

        /*
         * Acquire poolSize channels
         */
        Channel[] ch = new Channel[poolSize];
        for (int i = 0; i < poolSize; i++) {
            ch[i] = getChannel(pool);
        }

        /*
         * assertions on state of pool -- all channels are acquired
         */
        assertEquals(poolSize, pool.getAcquiredChannelCount());
        assertEquals(poolSize, pool.getTotalChannels());

        for (int i = 0; i < poolSize; i++) {
            releaseChannel(pool, ch[i]);
        }

        /*
         * pool is full, but no acquired channels
         */
        assertEquals(0, pool.getAcquiredChannelCount());
        assertEquals(poolSize, pool.getTotalChannels());

        /* prune and verify that no channels were removed */
        assertEquals(0, pool.pruneChannels());

        /*
         * sleep 3x inactivity period, check again,
         * verifying that all but poolMinSize channels are gone.
         * The pool sets the refresh task interval to the inactivity
         * period so it should prune the inactive channels
         */
        Thread.sleep(poolInactivityPeriod * 3000);
        assertEquals(poolMinSize, pool.getTotalChannels());

        Thread.sleep(poolInactivityPeriod * 2000);

        /* use 1s keepalive period to force it */
        assertEquals(poolMinSize, pool.doKeepAlive(1));
        client.shutdown();
    }

    /*
     * This test requires a valid cloud service endpoint. A non-production
     * endpoint is best. It should be provided via
     *  -Dtest.cloudendpoint=...
     * This is needed to test the behavior of the pool in the face of the
     * 65s idle timeout from the load balancer service as well as the keepalive
     * logic in the pool.
     *
     * The goal is to end this test with the min size (2) channels in the
     * pool. It does this:
     * o configures min size 4 and a keepalive callback using the HttpClient
     * o configures inactivity period to -1, meaning, don't prune the pool
     * locally
     * o acquires and releases 4 channels
     * o sleeps for > 65s
     * o prunes
     * o verify:
     *   1. 2 channels are gone because the LBaaS closed them
     *   2. 2 channels are active/alive because of the keepalive on the min size
     */
    @Test
    public void testCloudTimeout() throws Exception {
        final int poolSize = 4;
        final int poolMinSize = 2;
        final String endpoint = System.getProperty("test.cloudendpoint");
        final int port = 443;
        final int sleepTimeMs = 70000;

        Assume.assumeTrue(endpoint != null);

        /* serviceURL is used in the test but a handle is not required */
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        serviceURL = config.getServiceURL();

        HttpClient client = new HttpClient(
            endpoint,
            port,
            0, // threads
            poolMinSize,
            -1, // poolInactivityPeriod
            0, // contentLen
            0, // chunkSize
            buildSslContext(),
            0,    // ssl handshake timeout
            "Pool Cloud Test",
            logger);

        ConnectionPool pool = client.getConnectionPool();

        pool.setKeepAlive(new ConnectionPool.KeepAlive() {
                    @Override
                    public boolean keepAlive(Channel ch) {
                        return client.doKeepAlive(ch);
                    }
                });

        /*
         * Acquire poolSize channels, then release them to the pool. Do this
         * 2x to bump the use count on the channels
         */
        Channel[] ch = new Channel[poolSize];
        for (int count = 0; count < 2; count++) {
            for (int i = 0; i < poolSize; i++) {
                ch[i] = getChannel(pool);
            }
            for (int i = 0; i < poolSize; i++) {
                releaseChannel(pool, ch[i]);
            }
        }

        /*
         * pool is full, but no acquired channels
         */
        assertEquals(0, pool.getAcquiredChannelCount());
        assertEquals(poolSize, pool.getTotalChannels());

        /* wait for LBaas idle period (65s) and a bit more */
        Thread.sleep(sleepTimeMs);

        /* assert that 2 channels have gone inactive and been pruned */
        assertEquals(poolSize - poolMinSize, pool.getTotalChannels());

        /* assert that the number of channels is the min size configured */
        assertEquals(poolMinSize, pool.getTotalChannels());

        client.shutdown();
    }

    @Test
    public void testMetricsAndReuse() throws Exception {
        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
            .group(group)
            .channel(LocalChannel.class)
            .remoteAddress(address);

        /* A dummy user handler (noop) */
        ConnectionPool pool = getConnectionPool(bootstrap, 0, 2, 2);

        /* CHECK 1: Initial */
        assertStats(pool, 0, 0, 0, 0);

        /* CHECK 2: Acquire */
        Channel ch1 = pool.acquire().sync().getNow();
        /* Total:1, Acquired:1, Idle:0 */
        assertStats(pool, 1, 1, 0, 0);

        /* CHECK 3: Release */
        pool.release(ch1);
        /* Total:1, Acquired:0, Idle:1 */
        assertStats(pool, 1, 0, 1, 0);

        /* CHECK 4: Reuse */
        Channel ch2 = pool.acquire().sync().getNow();
        /* Should be the SAME channel object (reused) */
        assertEquals(ch1.id(), ch2.id());
        /* Stats: Total:1, Acquired:1, Idle:0 */
        assertStats(pool, 1, 1, 0, 0);

        /* acquire another channel and check acquire count is 2 */
        Channel ch3 = pool.acquire().sync().getNow();
        /* Stats: Total:2, Acquired:2, Idle:0 */
        assertStats(pool, 2, 2, 0, 0);

        /* Try to acquire another channel, this should be put into pending */
        Future<Channel> ch4 = pool.acquire();
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:1 */
        assertStats(pool, 2, 2, 0, 1);

        /* Try to acquire another channel, this should be put into pending */
        Future<Channel> ch5 = pool.acquire();
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:2 */
        assertStats(pool, 2, 2, 0, 2);

        /* try to acquire more than max pending and check error is thrown */
        Assert.assertThrows(IllegalStateException.class,
                            ()-> pool.acquire().sync().getNow());
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:2 */
        assertStats(pool, 2, 2, 0, 2);

        /* Release back a channel and verify that pending is reduced*/
        pool.release(ch2);
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:1 */
        Thread.sleep(10);
        assertStats(pool, 2, 2, 0, 1);
        assertTrue(ch4.isSuccess());

        /* Release back a channel and verify that pending is reduced*/
        pool.release(ch3);
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:0 */
        Thread.sleep(10);
        assertStats(pool, 2, 2, 0, 0);
        assertTrue(ch5.isSuccess());

        /* Release back a channel and verify Idle is increased */
        pool.release(ch4.getNow());
        /* Stats: Total:2, Acquired:1, Idle:1, Pending:0 */
        Thread.sleep(10);
        assertStats(pool, 2, 1, 1, 0);

        /* Release back a channel and verify Idle is increased */
        pool.release(ch5.getNow());
        /* Stats: Total:2, Acquired:0, Idle:2, Pending:0 */
        Thread.sleep(10);
        assertStats(pool, 2, 0, 2, 0);

        /* check pending tasks are completed when the pool is closed */
        ch1 = pool.acquire().sync().getNow();
        ch2 = pool.acquire().sync().getNow();
        ch4 = pool.acquire();
        /* Stats: Total:2, Acquired:2, Idle:0, Pending:1 */
        Thread.sleep(10);
        assertStats(pool, 2, 2, 0, 1);

        /* close the pool */
        pool.close();

        /* check pending ch4 is completed with exception */
        Thread.sleep(10);
        assertFalse(ch4.isSuccess());
        assertTrue(ch4.cause() instanceof RejectedExecutionException);
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
        ConnectionPool pool = getConnectionPool(bootstrap, 60, maxConnections,
                                                numberOfRequests + 1);
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
        assertEquals("Total should be capped at max",
                     maxConnections, pool.getTotalChannels());
        assertEquals("Acquired should be capped at max",
                     maxConnections, pool.getAcquiredChannelCount());
        assertEquals("Excess requests should be pending",
                     numberOfRequests - maxConnections,
                     pool.getPendingAcquires());

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

        /* Expect: Pending should be one now. */
        assertEquals("Pending queue should have 1", 1, pool.getPendingAcquires());

        threadPool.shutdown();
        pool.close();
    }

    private static ConnectionPool getConnectionPool(Bootstrap bootstrap,
                                                    int inactivityPeriodSeconds,
                                                    int maxConnections,
                                                    int numberOfRequests) {
        ChannelPoolHandler noopHandler = new ChannelPoolHandler() {
            @Override public void channelReleased(Channel ch) {}

            @Override public void channelAcquired(Channel ch) {}

            @Override public void channelCreated(Channel ch) {}
        };

        ConnectionPool pool =
            new ConnectionPool(bootstrap,
                               noopHandler,
                               logger,
                               false, /* isMinimal*/
                               0, /* pool min*/
                               inactivityPeriodSeconds, /* Inactivity seconds */
                               maxConnections,
                               numberOfRequests);
        return pool;
    }

    @Test
    public void testIdleEvictionInPool() throws InterruptedException {
        /* Create Pool */
        Bootstrap bootstrap = new Bootstrap()
            .group(group)
            .channel(LocalChannel.class)
            .remoteAddress(address);

        /* A dummy user handler (noop) */
        ConnectionPool pool = getConnectionPool(bootstrap, 2, 2, 5);

        /* 1. Acquire a channel */
        Channel ch = pool.acquire().sync().getNow();

        /* 2. Release it back to the pool (This starts the Idle Timer) */
        pool.release(ch);

        /* Verify it's currently Idle */
        assertTrue(ch.isOpen());
        assertEquals(1, pool.getFreeChannels());

        /* 3. SIMULATE the channel close */
        ch.close();

        /* 4. wait for the refresh task to close the channel */
        Thread.sleep(3000);

        /* The metrics should update (Total drops to 0) */
        assertEquals("Total count should drop to 0",
                     0, pool.getTotalChannels());
        assertEquals("Idle count should drop to 0",
                     0, pool.getFreeChannels());
    }


    private static Logger getLogger() {
        Logger tlogger = Logger.getLogger("oracle.nosql");
        String level = System.getProperty("test.loglevel");
        if (level == null) {
            level = "WARNING";
        }
        Level propLevel = Level.parse(level);
        Level tLevel = tlogger.getLevel();
        /* don't *decrease* the logging; logging decreases with higher values */
        if (tLevel == null ||
            propLevel.intValue() < tLevel.intValue()) {
            tlogger.setLevel(propLevel);
        }
        return tlogger;
    }

    /*
     * internal method to acquire a channel from the pool that does little/no
     * error handling as it expects success (except when the caller expects
     * failure
     */
    private Channel getChannel(ConnectionPool pool) throws Exception {
        Future<Channel> fut = pool.acquire();
        Channel ch = fut.get();
        assert ch.isActive();
        return ch;
    }

    private void releaseChannel(ConnectionPool pool, Channel ch) {
        pool.release(ch);
    }

    private SslContext buildSslContext() {
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            //builder.sessionTimeout(...);
            //builder.sessionCacheSize(...);
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException(
                "Unable o create SSL context: " + e);
        }
    }

    private void assertStats(ConnectionPool pool, int t,
                             int a, int i, int p) {
        assertEquals("Total", t, pool.getTotalChannels());
        assertEquals("Acquired", a, pool.getAcquiredChannelCount());
        assertEquals("Idle", i, pool.getFreeChannels());
        assertEquals("Pending", p, pool.getPendingAcquires());
    }
}
