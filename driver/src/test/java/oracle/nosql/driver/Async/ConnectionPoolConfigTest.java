/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.Async;

import oracle.nosql.driver.httpclient.ConnectionPoolConfig;
import org.junit.Test;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ConnectionPoolConfig}
 */
public class ConnectionPoolConfigTest {
    @Test
    public void test() {
        // default configs
        try {
            ConnectionPoolConfig cfg = ConnectionPoolConfig.builder().build();
            assertEquals(100, cfg.getMaxConnections());
            assertEquals(60000, cfg.getMaxIdleTime());
            assertEquals(300000, cfg.getMaxLifetime());
            assertEquals(-1, cfg.getMaxPendingAcquires());
            assertEquals(45000, cfg.getPendingAcquireTimeout());

            ConnectionProvider.builder("default-config")
                .maxConnections(cfg.getMaxConnections())
                .pendingAcquireMaxCount(cfg.getMaxPendingAcquires())
                .pendingAcquireTimeout(Duration.ofMillis(cfg.getPendingAcquireTimeout()))
                .maxLifeTime(Duration.ofMillis(cfg.getMaxLifetime()))
                .maxIdleTime(Duration.ofMillis(cfg.getMaxIdleTime()))
                .build();
        } catch (Exception e) {
            fail("Didn't expect any error but got " + e);
        }


        // non-default configs
        try {
            ConnectionPoolConfig cfg = ConnectionPoolConfig.builder()
                .maxConnections(10)
                .maxIdleTime(30000)
                .maxLifetime(60000)
                .maxPendingAcquires(50)
                .pendingAcquireTimeout(20000)
                .build();
            assertEquals(10, cfg.getMaxConnections());
            assertEquals(30000, cfg.getMaxIdleTime());
            assertEquals(60000, cfg.getMaxLifetime());
            assertEquals(50, cfg.getMaxPendingAcquires());
            assertEquals(20000, cfg.getPendingAcquireTimeout());

            ConnectionProvider.builder("default-config")
                .maxConnections(cfg.getMaxConnections())
                .pendingAcquireMaxCount(cfg.getMaxPendingAcquires())
                .pendingAcquireTimeout(Duration.ofMillis(cfg.getPendingAcquireTimeout()))
                .maxLifeTime(Duration.ofMillis(cfg.getMaxLifetime()))
                .maxIdleTime(Duration.ofMillis(cfg.getMaxIdleTime()))
                .build();

        } catch (Exception e) {
            fail("Didn't expect any error but got " + e);
        }
    }

    @Test
    public void testError() {
        // zero max connections
        try {
            ConnectionPoolConfig.builder()
                .maxConnections(0)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // negative max connection
        try {
            ConnectionPoolConfig.builder()
                .maxConnections(-1)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // zero max pending
        try {
            ConnectionPoolConfig.builder()
                .maxPendingAcquires(0)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // negative max pending acquires apart from -1;
        try {
            ConnectionPoolConfig.builder()
                .maxPendingAcquires(-2)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // zero max idle time
        try {
            ConnectionPoolConfig.builder()
                .maxIdleTime(0)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // negative max idle time
        try {
            ConnectionPoolConfig.builder()
                .maxIdleTime(-1)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // zero max life time
        try {
            ConnectionPoolConfig.builder()
                .maxLifetime(0)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // negative max life time
        try {
            ConnectionPoolConfig.builder()
                .maxLifetime(-1)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // zero  acquire timeout
        try {
            ConnectionPoolConfig.builder()
                .pendingAcquireTimeout(0)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // negative  acquire timeout
        try {
            ConnectionPoolConfig.builder()
                .pendingAcquireTimeout(-1)
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }
}
