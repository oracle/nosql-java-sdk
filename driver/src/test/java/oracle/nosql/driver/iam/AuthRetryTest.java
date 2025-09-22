/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslContext;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.values.MapValue;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class AuthRetryTest extends DriverTestBase {
    private TestHttpClient testHttpClient;

    @Before
    public void setUp() {
        testHttpClient = new TestHttpClient();
    }

    @Test
    public void testInvalidAuthorizationExceptionRetry()
        throws Exception {

        TestClient client = getTestClient();

        Request request = new GetRequest().setTableName("foo")
            .setKey(new MapValue().put("foo", "bar"));

        /* Expect the InvalidAuthorizationException is retried once only */
        assertThrows(InvalidAuthorizationException.class,
                     () -> client.execute(request));
        assertEquals(3, testHttpClient.execCount.get());
        assertEquals(2, testHttpClient.iaeCount.get());
        assertEquals(1,
                     request.getRetryStats()
                         .getNumExceptions(
                             InvalidAuthorizationException.class));
    }

    private TestClient getTestClient() {
        AuthorizationProvider provider =
            new AuthorizationProvider() {
                @Override
                public String getAuthorizationString(Request request) {
                    return "Test";
                }

                @Override
                public void close() {
                }
            };
        NoSQLHandleConfig cf = new NoSQLHandleConfig("http://localhost:8080");
        cf.setAuthorizationProvider(provider);
        return new TestClient(null, cf);
    }

    private class TestClient extends Client {
        public TestClient(Logger logger, NoSQLHandleConfig config) {
            super(logger, config);
        }

        @Override
        public HttpClient createHttpClient(URL url,
                                           NoSQLHandleConfig httpConfig,
                                           SslContext sslCtx,
                                           Logger logger) {
            return testHttpClient;
        }
    }

    /* Mock HttpClient */
    private static class TestHttpClient extends HttpClient {
        private final AtomicInteger execCount = new AtomicInteger(0);
        private final AtomicInteger iaeCount = new AtomicInteger(0);

        public TestHttpClient() {
            super("localhost", 8080, 1, 0, 0, 0, 0, null, 0, "test", null);
        }

        @Override
        public void runRequest(HttpRequest request,
                               ResponseHandler handler,
                               Channel channel) {
            /*
             * Simulate an authentication failure scenario where the initial
             * attempt throws SecurityInfoNotReadyException, and subsequent
             * retries throw InvalidAuthorizationException.
             */
            int count = execCount.incrementAndGet();
            if (count == 1) {
                throw new SecurityInfoNotReadyException("test");
            } else {
                iaeCount.incrementAndGet();
                throw new InvalidAuthorizationException("test");
            }
        }

        @Override
        public Channel getChannel(int timeoutMs) {
            /*
             * Utilize Netty's EmbeddedChannel to create a mock channel that
             * remains active, enabling the request execution to proceed with
             * a valid channel for error simulation purposes.
             */
            return new EmbeddedChannel() {
                @Override
                public boolean isActive() {
                    return true;
                }
            };
        }
    }
}
