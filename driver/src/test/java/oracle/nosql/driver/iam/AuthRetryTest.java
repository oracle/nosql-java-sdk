/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
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
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.kv.OAuthAccessTokenProvider;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.values.MapValue;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

    @Test
    public void testOAuthAuthenticationExceptionRetry()
        throws Exception {

        testHttpClient.oauthFailures =
            new OAuthFailure[] {
                OAuthFailure.AUTHENTICATION,
                OAuthFailure.AUTHENTICATION
            };
        TestOAuthProvider provider = new TestOAuthProvider();
        TestClient client = getTestClient(provider);

        Request request = new GetRequest().setTableName("foo")
            .setKey(new MapValue().put("foo", "bar"));

        /*
         * Expect the AuthenticationException for OAuth to be retried once only.
         * The second AuthenticationException should be returned immediately,
         * not retried until request timeout.
         */
        assertThrows(AuthenticationException.class,
                     () -> client.execute(request));
        assertEquals(2, testHttpClient.execCount.get());
        assertEquals(2, testHttpClient.authenticationExceptionCount.get());
        assertEquals(1, provider.invalidationCount.get());
        assertEquals("Bearer Test-1", provider.lastInvalidated.get());
        assertEquals(0, provider.flushCount.get());
        assertEquals(1,
                     request.getRetryStats()
                         .getNumExceptions(AuthenticationException.class));
    }

    @Test
    public void testOAuthAlternatingAuthenticationThenAuthorization() {
        assertOAuthFailureSequence(
            OAuthFailure.AUTHENTICATION,
            OAuthFailure.INVALID_AUTHORIZATION,
            InvalidAuthorizationException.class);
    }

    @Test
    public void testOAuthAlternatingAuthorizationThenAuthentication() {
        assertOAuthFailureSequence(
            OAuthFailure.INVALID_AUTHORIZATION,
            OAuthFailure.AUTHENTICATION,
            AuthenticationException.class);
    }

    @Test
    public void testOAuthAuthenticationSubclassRetryIsBounded() {
        assertOAuthFailureSequence(
            OAuthFailure.AUTHENTICATION_SUBCLASS,
            OAuthFailure.INVALID_AUTHORIZATION,
            InvalidAuthorizationException.class);
    }

    @Test
    public void testOAuthAuthorizationSubclassRetryIsBounded() {
        assertOAuthFailureSequence(
            OAuthFailure.INVALID_AUTHORIZATION_SUBCLASS,
            OAuthFailure.AUTHENTICATION,
            AuthenticationException.class);
    }

    private void assertOAuthFailureSequence(
        OAuthFailure first,
        OAuthFailure second,
        Class<? extends RuntimeException> expectedClass) {

        testHttpClient.oauthFailures = new OAuthFailure[] { first, second };
        TestOAuthProvider provider = new TestOAuthProvider();
        TestClient client = getTestClient(provider);
        Request request = new GetRequest().setTableName("foo")
            .setKey(new MapValue().put("foo", "bar"));

        assertThrows(expectedClass, () -> client.execute(request));
        assertEquals(2, testHttpClient.execCount.get());
        assertEquals(1, provider.invalidationCount.get());
        assertEquals("Bearer Test-1", provider.lastInvalidated.get());
        assertEquals("Bearer Test-2", provider.authorization.get());
        assertEquals(0, provider.flushCount.get());
        assertEquals(1, request.getRetryStats().getRetries());
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
        return getTestClient(provider);
    }

    private TestClient getTestClient(AuthorizationProvider provider) {
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
        private final AtomicInteger authenticationExceptionCount =
            new AtomicInteger(0);
        private OAuthFailure[] oauthFailures;

        public TestHttpClient() {
            super("localhost", 8080, 1, 0, 0, 0, 0, null, 0, "test", null);
        }

        @Override
        public void runRequest(HttpRequest request,
                               ResponseHandler handler,
                               Channel channel) {
            if (oauthFailures != null) {
                final int index = execCount.getAndIncrement();
                final OAuthFailure failure =
                    oauthFailures[Math.min(index, oauthFailures.length - 1)];
                if (failure == OAuthFailure.AUTHENTICATION ||
                    failure == OAuthFailure.AUTHENTICATION_SUBCLASS) {
                    authenticationExceptionCount.incrementAndGet();
                    if (failure == OAuthFailure.AUTHENTICATION_SUBCLASS) {
                        throw new TestAuthenticationException("test");
                    }
                    throw new AuthenticationException("test");
                }
                iaeCount.incrementAndGet();
                if (failure ==
                    OAuthFailure.INVALID_AUTHORIZATION_SUBCLASS) {
                    throw new TestInvalidAuthorizationException("test");
                }
                throw new InvalidAuthorizationException("test");
            }

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

    private static class TestOAuthProvider extends OAuthAccessTokenProvider {

        private final AtomicReference<String> authorization =
            new AtomicReference<String>("Bearer Test-1");
        private final AtomicReference<String> lastInvalidated =
            new AtomicReference<String>();
        private final AtomicInteger invalidationCount = new AtomicInteger(0);
        private final AtomicInteger flushCount = new AtomicInteger(0);

        @Override
        public String getAuthorizationString(Request request) {
            return authorization.get();
        }

        @Override
        public boolean invalidateAuthorizationString(
            String failedAuthorization) {

            invalidationCount.incrementAndGet();
            lastInvalidated.set(failedAuthorization);
            return authorization.compareAndSet(
                failedAuthorization, "Bearer Test-2");
        }

        @Override
        public void flushCache() {
            flushCount.incrementAndGet();
        }

        @Override
        protected String getAccessToken() {
            return "Test";
        }
    }

    private enum OAuthFailure {
        AUTHENTICATION,
        AUTHENTICATION_SUBCLASS,
        INVALID_AUTHORIZATION,
        INVALID_AUTHORIZATION_SUBCLASS
    }

    private static class TestAuthenticationException
        extends AuthenticationException {

        private static final long serialVersionUID = 1L;

        private TestAuthenticationException(String message) {
            super(message);
        }
    }

    private static class TestInvalidAuthorizationException
        extends InvalidAuthorizationException {

        private static final long serialVersionUID = 1L;

        private TestInvalidAuthorizationException(String message) {
            super(message);
        }
    }
}
