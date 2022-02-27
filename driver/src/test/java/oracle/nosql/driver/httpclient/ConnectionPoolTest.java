/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.httpclient;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import oracle.nosql.driver.ProxyTestBase;

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
public class ConnectionPoolTest extends ProxyTestBase {

    @Test
    public void poolTest() throws Exception {
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
            "Pool Test",
            getLogger());

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
        Channel ch[] = new Channel[poolSize];
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
         * sleep 2x inactivity period, prune again,
         * verifying that all but poolMinSize channels are gone
         */
        Thread.sleep(poolInactivityPeriod * 2000);

        assertEquals(poolSize - poolMinSize, pool.pruneChannels());
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

        if (endpoint == null) {
            throw new IllegalStateException(
                "testCloudTimeout requires setting of the system property, " +
                "\"test.cloudendpoint\"");
        }

        HttpClient client = new HttpClient(
            endpoint,
            port,
            0, // threads
            poolMinSize,
            -1, // poolInactivityPeriod
            0, // contentLen
            0, // chunkSize
            buildSslContext(),
            "Pool Cloud Test",
            getLogger());

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
        Channel ch[] = new Channel[poolSize];
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
        assertEquals(poolSize - poolMinSize, pool.pruneChannels());

        /* assert that the number of channels is the min size configured */
        assertEquals(poolMinSize, pool.getTotalChannels());

        client.shutdown();
    }

    private Logger getLogger() {
        Logger logger = Logger.getLogger(getClass().getName());
        String level = System.getProperty("test.loglevel");
        if (level == null) {
            level = "WARNING";
        }
        logger.setLevel(Level.parse(level));
        return logger;
    }

    /*
     * internal method to acquire a channel from the pool that does little/no
     * error handling as it expects success (except when the caller expects
     * failure
     */
    private Channel getChannel(ConnectionPool pool) throws Exception {
        Future<Channel> fut = pool.acquire();
        return fut.get();
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
}
