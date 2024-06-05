/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.Async;

import io.netty.handler.ssl.SslContextBuilder;
import oracle.nosql.driver.httpclient.ConnectionPoolConfig;
import oracle.nosql.driver.httpclient.ReactorHttpClient;
import org.junit.Test;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.ProxyProvider;

import javax.net.ssl.SSLException;
import java.net.SocketAddress;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link ReactorHttpClient}
 */
public class ReactorHttpClientTest {
    @Test
    public void testDefaultConfig() {
        // default builder
        ReactorHttpClient httpClient = ReactorHttpClient.builder().build();

        //verify default host:port
        SocketAddress address =  httpClient.getHttpClient().configuration()
            .remoteAddress().get();
        String hostPort = address.toString();
        assertTrue(hostPort.contains("localhost"));
        assertTrue(hostPort.contains("80"));

        //verify ssl is null by default
        assertNull(httpClient.getHttpClient().configuration().sslProvider());

        //verify default decoder config
        assertEquals(4096,
            httpClient.getHttpClient().configuration()
                .decoder().maxInitialLineLength());
        assertEquals(8192, httpClient.getHttpClient().configuration()
                .decoder().maxHeaderSize());
        assertEquals(65536, httpClient.getHttpClient().configuration()
                .decoder().maxChunkSize());

        //verify default logger
        assertEquals(ReactorHttpClient.class.getName(),
                httpClient.getLogger().getName());

        //verify default connection pool config
        ConnectionProvider defaultProvider =
                httpClient.getHttpClient().configuration().connectionProvider();
        assertEquals(100, defaultProvider.maxConnections());

        //verify proxy is not set
        assertFalse(httpClient.getHttpClient().configuration().hasProxy());

        //verify loop resources is not null
        assertNotNull(httpClient.getHttpClient().configuration().loopResources());
    }

    @Test
    public void testCustomConfig() throws SSLException {
        ConnectionPoolConfig poolConfig = ConnectionPoolConfig.builder()
            .maxConnections(500)
            .maxPendingAcquires(1000)
            .pendingAcquireTimeout(60000)
            .maxIdleTime(20000)
            .maxLifetime(40000)
            .build();

        Logger logger = Logger.getLogger(getClass().getName());

        ReactorHttpClient httpClient = ReactorHttpClient.builder()
            .host("https://nosql.us-phoenix-1.oci.oraclecloud.com")
            .port(443)
            .sslContext(SslContextBuilder.forClient().build())
            .sslHandshakeTimeoutMs(20000)
            .maxContentLength(1000)
            .maxChunkSize(8192)
            .maxHeaderSize(1024)
            .maxInitialLineLength(512)
            .connectionPoolConfig(poolConfig)
            .logger(logger)
            .build();

        //verify host and port
        SocketAddress address =  httpClient.getHttpClient().configuration()
                .remoteAddress().get();
        String hostPort = address.toString();
        assertTrue(hostPort.contains("https://nosql.us-phoenix-1.oci.oraclecloud.com"));
        assertTrue(hostPort.contains("443"));

        //verify ssl is set
        assertNotNull(httpClient.getHttpClient().configuration().sslProvider());

        //verify default decoder config
        assertEquals(512,
                httpClient.getHttpClient().configuration()
                        .decoder().maxInitialLineLength());
        assertEquals(1024, httpClient.getHttpClient().configuration()
                .decoder().maxHeaderSize());
        assertEquals(8192, httpClient.getHttpClient().configuration()
                .decoder().maxChunkSize());

        //verify logger
        assertSame(logger, httpClient.getLogger());

        //verify connection pool config
        ConnectionProvider connectionProvider =
                httpClient.getHttpClient().configuration().connectionProvider();
        assertEquals(500, connectionProvider.maxConnections());

        //verify proxy is not set
        assertFalse(httpClient.getHttpClient().configuration().hasProxy());

        //verify loop resources is not null
        assertNotNull(httpClient.getHttpClient().configuration().loopResources());
    }

    @Test
    public void testMinimalClient() {
        ReactorHttpClient client = ReactorHttpClient.createMinimalClient(
            "host1",
            5000,
            null,
            5000,
            "minimal-client",
            null);

        //verify default host and port
        SocketAddress address =  client.getHttpClient().configuration()
            .remoteAddress().get();
        String hostPort = address.toString();
        assertTrue(hostPort.contains("host1"));
        assertTrue(hostPort.contains("5000"));

        //verify ssl is null
        assertNull(client.getHttpClient().configuration().sslProvider());

        //verify default decoder config
        assertEquals(4096,
                client.getHttpClient().configuration()
                        .decoder().maxInitialLineLength());
        assertEquals(8192, client.getHttpClient().configuration()
                .decoder().maxHeaderSize());
        assertEquals(65536, client.getHttpClient().configuration()
                .decoder().maxChunkSize());

        // verify connection is 1
        assertEquals(1,
            client.getHttpClient().configuration().connectionProvider()
            .maxConnections());
    }

    @Test
    public void testProxy() {
        ReactorHttpClient httpClient = ReactorHttpClient.builder()
            .proxyHost("us.oracle.com")
            .proxyPort(80)
            .proxyUsername("oracle")
            .proxyPassword("oracle123")
            .build();

        ProxyProvider proxyProvider =
            httpClient.getHttpClient().configuration().proxyProvider();
        assertNotNull(proxyProvider);
        assertEquals("us.oracle.com",
                proxyProvider.getAddress().get().getHostName());
        assertEquals(80,
                proxyProvider.getAddress().get().getPort());
        assertEquals("oracle", httpClient.getProxyUser());
        assertEquals("oracle123", httpClient.getProxyPassword());
    }

    @Test
    public void testError() {
        //host null
        try {
            ReactorHttpClient.builder().host(null).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //host empty
        try {
            ReactorHttpClient.builder().host(null).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //port 0
        try {
            ReactorHttpClient.builder().port(0).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //port negative
        try {
            ReactorHttpClient.builder().port(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //max content length negative
        try {
            ReactorHttpClient.builder().maxContentLength(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //max initial line length negative
        try {
            ReactorHttpClient.builder().maxInitialLineLength(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //max header size negative
        try {
            ReactorHttpClient.builder().maxHeaderSize(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //max chunk size negative
        try {
            ReactorHttpClient.builder().maxChunkSize(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //ssl handshake negative
        try {
            ReactorHttpClient.builder().sslHandshakeTimeoutMs(-1).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //proxy only host is set
        try {
            ReactorHttpClient.builder().proxyHost("host").build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //proxy host is set but port is 0
        try {
            ReactorHttpClient.builder().proxyHost("host").proxyPort(0).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //proxy port is set not host
        try {
            ReactorHttpClient.builder().proxyPort(8080).build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //proxy username is set not password
        try {
            ReactorHttpClient.builder()
                .host("host").port(8080)
                .proxyUsername("username")
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }

        //proxy pwd is set not username
        try {
            ReactorHttpClient.builder()
                .host("host").port(8080)
                .proxyPassword("password")
                .build();
            fail("Expecting IllegalArgumentException but didn't get");
        } catch (IllegalArgumentException ignored) {
            //expected
        }
    }
}
