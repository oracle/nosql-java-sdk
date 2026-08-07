/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.kv;

import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.KV_SECURITY_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.values.JsonUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("restriction")
public class OAuthAccessTokenProviderTest {

    private static final String loginPath = KV_SECURITY_PATH + "/oauthlogin";
    private static final String logoutPath = KV_SECURITY_PATH + "/logout";

    private static final int port = 1444;
    private static final String endpoint = "https://localhost:" + port;

    private static final String oauthAccessToken = "OCI_ACCESS_TOKEN";
    private static final String secondOAuthAccessToken = "OCI_ACCESS_TOKEN_2";
    private static final String loginToken = "OAUTH_LOGIN_TOKEN";
    private static final String reloginToken = "OAUTH_RELOGIN_TOKEN";
    private static final String loginIssuer =
        "https://issuer.example.com/tenant";
    private static final String loginSubjectType = "user";
    private static final String loginSubjectId = "oauth-data/it@test.com";
    private static final String differentSubjectId =
        "oauth-data/other@test.com";
    private static final String authTokenPrefix = "Bearer ";

    private static HttpServer server;
    private static final AtomicInteger loginCounter = new AtomicInteger();
    private static final AtomicInteger logoutCounter = new AtomicInteger();
    private static volatile String lastLogoutToken;
    private static volatile String reloginIssuer = loginIssuer;
    private static volatile String reloginSubjectType = loginSubjectType;
    private static volatile String reloginSubjectId = loginSubjectId;
    private static volatile boolean omitAuthenticatedIdentity;
    private static volatile long loginTokenLifetimeMs = 15_000;
    private static volatile long loginDelayMs;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        OAuthAccessTokenProvider.disableSSLHook = true;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.start();

        server.createContext(loginPath, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange)
                throws IOException {
                final String authString =
                    exchange.getRequestHeaders().get(AUTHORIZATION).get(0);
                assertTrue(authString.startsWith(authTokenPrefix));
                final int count = loginCounter.incrementAndGet();
                if (count == 1) {
                    assertEquals(authTokenPrefix + oauthAccessToken,
                                 authString);
                } else {
                    assertEquals(authTokenPrefix + secondOAuthAccessToken,
                                 authString);
                }
                final long delayMs = loginDelayMs;
                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Login handler interrupted", ie);
                    }
                }
                if (count == 1) {
                    generateLoginToken(
                        loginToken,
                        omitAuthenticatedIdentity ? null : loginIssuer,
                        loginSubjectType,
                        loginSubjectId,
                        exchange);
                } else {
                    generateLoginToken(reloginToken, reloginIssuer,
                                       reloginSubjectType,
                                       reloginSubjectId, exchange);
                }
            }
        });

        server.createContext(logoutPath, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange)
                throws IOException {
                final String authString =
                    exchange.getRequestHeaders().get(AUTHORIZATION).get(0);
                assertTrue(authString.startsWith(authTokenPrefix));
                lastLogoutToken = readTokenFromAuth(authString);
                logoutCounter.incrementAndGet();
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                exchange.close();
            }
        });
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        OAuthAccessTokenProvider.disableSSLHook = false;
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testBasic() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        resetAuthenticatedIdentity();
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertNotNull(authString);
            assertTrue(authString.startsWith(authTokenPrefix));
            assertEquals(loginToken, readTokenFromAuth(authString));

            Thread.sleep(10000);

            final String authReloginString =
                provider.getAuthorizationString(null);
            assertEquals(reloginToken,
                         readTokenFromAuth(authReloginString));

            provider.close();
            assertNull(provider.getAuthorizationString(null));
        } finally {
            provider.close();
        }

        tryBadEndpoint("http://localhost");
        tryBadEndpoint("localhost:8080");
        tryBadEndpoint("foo://localhost");
    }

    @Test
    public void testDisableAutoRenew() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertNotNull(authString);
            assertEquals(loginToken, readTokenFromAuth(authString));

            Thread.sleep(10000);

            final String sameAuthString =
                provider.getAuthorizationString(null);
            assertEquals(loginToken, readTokenFromAuth(sameAuthString));
            assertEquals(1, loginCounter.get());
        } finally {
            provider.close();
        }
    }

    @Test
    public void testLoginTokenExpiryControlsRefresh() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        loginTokenLifetimeMs = 12_000;
        TestProvider provider = new TestProvider(60);
        provider.setEndpoint(endpoint);

        try {
            assertEquals(loginToken, readTokenFromAuth(
                provider.getAuthorizationString(null)));

            waitForAuthorizationToken(provider, reloginToken, 5_000);
            assertTrue(loginCounter.get() >= 2);
        } finally {
            loginTokenLifetimeMs = 15_000;
            provider.close();
        }
    }

    @Test
    public void testRefreshFailureRetainsLoginToken() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        FailingRefreshProvider provider = new FailingRefreshProvider();
        provider.setEndpoint(endpoint);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertEquals(loginToken, readTokenFromAuth(authString));

            provider.waitForRefreshAttempt(5_000);

            assertEquals(authString, provider.getAuthorizationString(null));
            assertEquals(1, loginCounter.get());
        } finally {
            provider.close();
        }
    }

    @Test
    public void testLoginUsesRequestTimeout() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        loginDelayMs = 500;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);
        final long startNanos = System.nanoTime();

        try {
            provider.getAuthorizationString(new GetRequest().setTimeout(50));
            fail("OAuth login should have observed the request timeout");
        } catch (NoSQLException expected) {
            final long elapsedMs =
                (System.nanoTime() - startNanos) / 1_000_000;
            assertTrue("OAuth login exceeded request timeout: " + elapsedMs,
                       elapsedMs < loginDelayMs);
        } finally {
            Thread.sleep(loginDelayMs + 100);
            loginDelayMs = 0;
            provider.close();
        }
    }

    @Test
    public void testFlushCacheRelogin() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertEquals(loginToken, readTokenFromAuth(authString));

            provider.flushCache();

            final String authReloginString =
                provider.getAuthorizationString(null);
            assertEquals(reloginToken,
                         readTokenFromAuth(authReloginString));
            assertEquals(2, loginCounter.get());
        } finally {
            provider.close();
        }
    }

    @Test
    public void testReloginWithDifferentSubjectIdFails() throws Exception {
        assertReloginIdentityRejected(loginIssuer, loginSubjectType,
                                      differentSubjectId);
    }

    @Test
    public void testReloginWithDifferentIssuerFails() throws Exception {
        assertReloginIdentityRejected("https://other.example.com/tenant",
                                      loginSubjectType, loginSubjectId);
    }

    @Test
    public void testReloginWithDifferentSubjectTypeFails() throws Exception {
        assertReloginIdentityRejected(loginIssuer, "client", loginSubjectId);
    }

    private void assertReloginIdentityRejected(String issuer,
                                               String subjectType,
                                               String subjectId)
        throws Exception {

        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        resetAuthenticatedIdentity();
        reloginIssuer = issuer;
        reloginSubjectType = subjectType;
        reloginSubjectId = subjectId;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertEquals(loginToken, readTokenFromAuth(authString));

            provider.flushCache();

            provider.getAuthorizationString(null);
            fail("Relogin with a different identity should have failed");
        } catch (InvalidAuthorizationException iae) {
            assertTrue(iae.getMessage().startsWith(
                "Logout required prior to logging in with new " +
                "user identity."));
        } finally {
            resetAuthenticatedIdentity();
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(reloginToken, lastLogoutToken);
    }

    @Test
    public void testLoginWithoutAuthenticatedIdentityFails() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        resetAuthenticatedIdentity();
        omitAuthenticatedIdentity = true;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            provider.getAuthorizationString(null);
            fail("Login without an authenticated identity should have failed");
        } catch (InvalidAuthorizationException iae) {
            assertTrue(iae.getMessage().startsWith(
                "Invalid OAuth login response: authenticated identity is " +
                "missing"));
        } finally {
            resetAuthenticatedIdentity();
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(loginToken, lastLogoutToken);
    }

    @Test
    public void testCloseLogsOutLoginToken() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        resetAuthenticatedIdentity();
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        final String authString = provider.getAuthorizationString(null);
        assertEquals(loginToken, readTokenFromAuth(authString));

        provider.close();

        assertNull(provider.getAuthorizationString(null));
        assertEquals(1, logoutCounter.get());
    }

    private void tryBadEndpoint(String ep) {
        TestProvider provider = new TestProvider();
        try {
            provider.setEndpoint(ep);
            fail("Endpoint should have failed: " + ep);
        } catch (IllegalArgumentException iae) {
            assertNull(provider.getEndpoint());
        }
    }

    private static void resetAuthenticatedIdentity() {
        omitAuthenticatedIdentity = false;
        reloginIssuer = loginIssuer;
        reloginSubjectType = loginSubjectType;
        reloginSubjectId = loginSubjectId;
    }

    private static void generateLoginToken(String tokenText,
                                           String issuer,
                                           String subjectType,
                                           String subjectId,
                                           HttpExchange exchange) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos);
             OutputStream os = exchange.getResponseBody()) {

            long expireTime =
                System.currentTimeMillis() + loginTokenLifetimeMs;
            oos.writeShort(1);
            oos.writeLong(expireTime);
            oos.writeBytes(tokenText);
            oos.flush();

            final String tokenString =
                JsonUtils.convertBytesToHex(baos.toByteArray());
            final String jsonString =
                "{\"token\":\"" + tokenString + "\"," +
                "\"expireAt\":" + expireTime +
                (issuer != null ?
                    ",\"authenticatedIdentity\":{" +
                    "\"type\":\"oauth\"," +
                    "\"issuer\":\"" + issuer + "\"," +
                    "\"subjectType\":\"" + subjectType + "\"," +
                    "\"subjectId\":\"" + subjectId + "\"}" : "") +
                "}";

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK,
                                         jsonString.length());
            os.write(jsonString.getBytes());
            os.flush();
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to encode", ioe);
        }
    }

    private static String readTokenFromAuth(String authString) {
        final String authEncoded =
            authString.substring(authTokenPrefix.length());
        final byte[] token = JsonUtils.convertHexToBytes(authEncoded);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(token);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            ois.readShort();
            ois.readLong();
            byte[] tokenBytes = new byte[ois.available()];
            ois.read(tokenBytes);
            return new String(tokenBytes);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to decode", ioe);
        }
    }

    private static void waitForAuthorizationToken(
        OAuthAccessTokenProvider provider,
        String expectedToken,
        long timeoutMs)
        throws InterruptedException {

        final long limit = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < limit) {
            final String authString = provider.getAuthorizationString(null);
            if (expectedToken.equals(readTokenFromAuth(authString))) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for refreshed OAuth login token");
    }

    private static class TestProvider extends OAuthAccessTokenProvider {

        private final AtomicInteger tokenCounter = new AtomicInteger();
        private final long expiresInSeconds;

        TestProvider() {
            this(15);
        }

        TestProvider(long expiresInSeconds) {
            this.expiresInSeconds = expiresInSeconds;
        }

        @Override
        protected AccessTokenInfo getAccessTokenInfo() {
            if (tokenCounter.incrementAndGet() == 1) {
                return new AccessTokenInfo(oauthAccessToken, expiresInSeconds);
            }
            return new AccessTokenInfo(secondOAuthAccessToken,
                                       expiresInSeconds);
        }
    }

    private static class FailingRefreshProvider
        extends OAuthAccessTokenProvider {

        private final AtomicInteger tokenCounter = new AtomicInteger();

        @Override
        protected AccessTokenInfo getAccessTokenInfo() {
            if (tokenCounter.incrementAndGet() == 1) {
                return new AccessTokenInfo(oauthAccessToken, 12);
            }
            throw new IllegalStateException("test refresh failure");
        }

        private void waitForRefreshAttempt(long timeoutMs)
            throws InterruptedException {

            final long limit = System.currentTimeMillis() + timeoutMs;
            while (tokenCounter.get() < 2 &&
                   System.currentTimeMillis() < limit) {
                Thread.sleep(50);
            }
            assertTrue("Timed out waiting for refresh callback",
                       tokenCounter.get() >= 2);
        }
    }
}
