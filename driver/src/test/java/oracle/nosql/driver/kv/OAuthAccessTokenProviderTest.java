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
import static org.junit.Assert.assertFalse;
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
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.values.JsonUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.AfterClass;
import org.junit.Before;
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
    private static volatile int loginStatus = HttpURLConnection.HTTP_OK;
    private static volatile String loginErrorBody = "";
    private static volatile String loginResponseOverride;
    private static volatile int logoutStatus = HttpURLConnection.HTTP_OK;
    private static volatile String logoutErrorBody = "";

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
                if (loginStatus != HttpURLConnection.HTTP_OK) {
                    sendResponse(exchange, loginStatus, loginErrorBody);
                    return;
                }
                if (loginResponseOverride != null) {
                    sendResponse(exchange, HttpURLConnection.HTTP_OK,
                                 loginResponseOverride);
                    return;
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
                sendResponse(exchange, logoutStatus, logoutErrorBody);
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

    @Before
    public void resetTestState() {
        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        resetAuthenticatedIdentity();
        loginTokenLifetimeMs = 15_000;
        loginDelayMs = 0;
        loginStatus = HttpURLConnection.HTTP_OK;
        loginErrorBody = "";
        loginResponseOverride = null;
        logoutStatus = HttpURLConnection.HTTP_OK;
        logoutErrorBody = "";
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
        TestProvider provider = new TestProvider();
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
        loginTokenLifetimeMs = 12_000;
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

    @Test
    public void testCallbackCloseStopsLogin() {
        ClosingCallbackProvider provider = new ClosingCallbackProvider();
        provider.setEndpoint(endpoint);

        assertNull(provider.getAuthorizationString(null));
        assertEquals(0, loginCounter.get());
        assertEquals(0, logoutCounter.get());
    }

    @Test
    public void testMissingAccessTokenRejectedBeforeLogin() {
        OAuthAccessTokenProvider provider =
            new OAuthAccessTokenProvider() {
                @Override
                protected String getAccessToken() {
                    return null;
                }
            };
        provider.setEndpoint(endpoint);

        try {
            provider.getAuthorizationString(null);
            fail("A missing access token should be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("access token"));
        } finally {
            provider.close();
        }
        assertEquals(0, loginCounter.get());
    }

    @Test
    public void testExpiredLoginTokenRejectedAndLoggedOut() {
        loginTokenLifetimeMs = -1_000;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint);

        try {
            provider.getAuthorizationString(null);
            fail("An expired login token should be rejected");
        } catch (InvalidAuthorizationException expected) {
            assertTrue(expected.getMessage().contains("expired login token"));
        } finally {
            provider.close();
        }
        assertEquals(1, loginCounter.get());
        assertEquals(1, logoutCounter.get());
        assertEquals(loginToken, lastLogoutToken);
    }

    @Test
    public void testMalformedIdentityRejectedAndLoggedOut() {
        final long expireAt = System.currentTimeMillis() + 60_000;
        final String encodedToken = encodeLoginToken(loginToken, expireAt);
        loginResponseOverride =
            "{\"token\":\"" + encodedToken + "\"," +
            "\"expireAt\":" + expireAt + "," +
            "\"authenticatedIdentity\":{" +
            "\"type\":\"oauth\"," +
            "\"issuer\":\"" + loginIssuer + "\"," +
            "\"subjectType\":\"user\"}}";
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            provider.getAuthorizationString(null);
            fail("A malformed authenticated identity should be rejected");
        } catch (InvalidAuthorizationException expected) {
            assertTrue(expected.getMessage().contains(
                "authenticated identity is invalid"));
        } finally {
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(loginToken, lastLogoutToken);
    }

    @Test
    public void testNonPositiveLoginExpiryRejectedAndLoggedOut() {
        final String encodedToken = encodeLoginToken(loginToken, 0);
        loginResponseOverride = createLoginResponse(
            encodedToken, 0, loginIssuer, loginSubjectType, loginSubjectId);
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            provider.getAuthorizationString(null);
            fail("A non-positive login-token expiration should be rejected");
        } catch (InvalidAuthorizationException expected) {
            assertTrue(expected.getMessage().contains("expired login token"));
        } finally {
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(loginToken, lastLogoutToken);
    }

    @Test
    public void testNullLoginTokenRejected() {
        final long expireAt = System.currentTimeMillis() + 60_000;
        loginResponseOverride =
            "{\"token\":null," +
            "\"expireAt\":" + expireAt + "," +
            "\"authenticatedIdentity\":{" +
            "\"type\":\"oauth\"," +
            "\"issuer\":\"" + loginIssuer + "\"," +
            "\"subjectType\":\"user\"," +
            "\"subjectId\":\"" + loginSubjectId + "\"}}";
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            provider.getAuthorizationString(null);
            fail("A null login token should be rejected");
        } catch (InvalidAuthorizationException expected) {
            assertTrue(expected.getMessage().contains(
                "Invalid OAuth login response"));
        } finally {
            provider.close();
        }
        assertEquals(0, logoutCounter.get());
    }

    @Test
    public void testPreconfiguredEndpointStillRequiresHttpsHandle() {
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint);
        final NoSQLHandleConfig config =
            new NoSQLHandleConfig("http://localhost:8080")
                .setAuthorizationProvider(provider);

        try {
            NoSQLHandleFactory.createNoSQLHandle(config);
            fail("An OAuth handle using HTTP should have been rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("service endpoint"));
        } finally {
            provider.close();
        }
    }

    @Test
    public void testPreconfiguredEndpointReceivesHandleSslContext()
        throws Exception {

        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint);
        final NoSQLHandleConfig config =
            new NoSQLHandleConfig("https://localhost:1445")
                .setAuthorizationProvider(provider);
        NoSQLHandle handle = null;

        try {
            handle = NoSQLHandleFactory.createNoSQLHandle(config);
            assertNotNull(getProviderField(provider, "sslContext"));
            assertEquals(endpoint, provider.getEndpoint());
        } finally {
            if (handle != null) {
                handle.close();
            } else {
                provider.close();
            }
        }
    }

    @Test
    public void testConditionalCacheInvalidation() {
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            final String first = provider.getAuthorizationString(null);
            provider.flushCache();
            final String second = provider.getAuthorizationString(null);

            assertFalse(provider.invalidateAuthorizationString(first));
            assertEquals(second, provider.getAuthorizationString(null));
            assertEquals(2, loginCounter.get());

            assertTrue(provider.invalidateAuthorizationString(second));
            assertNotNull(provider.getAuthorizationString(null));
            assertEquals(3, loginCounter.get());
        } finally {
            provider.close();
        }
    }

    @Test
    public void testCancelledRefreshRemovedFromQueue() throws Exception {
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint);

        try {
            assertNotNull(provider.getAuthorizationString(null));
            final ScheduledThreadPoolExecutor scheduler =
                getScheduler(provider);
            assertTrue(scheduler.getRemoveOnCancelPolicy());
            assertEquals(1, scheduler.getQueue().size());

            provider.flushCache();
            assertTrue(scheduler.getQueue().isEmpty());
        } finally {
            provider.close();
        }
    }

    @Test
    public void testRunningRefreshCancelledBeforeLogin() throws Exception {
        loginTokenLifetimeMs = 11_000;
        BlockingRefreshProvider provider = new BlockingRefreshProvider();
        provider.setEndpoint(endpoint);

        try {
            assertNotNull(provider.getAuthorizationString(null));
            assertTrue(provider.awaitRefreshCallback(5_000));

            final Thread disableRenewal =
                new Thread(() -> provider.setAutoRenew(false));
            disableRenewal.start();
            waitForAutoRenew(provider, false, 5_000);
            provider.releaseRefreshCallback();
            disableRenewal.join(5_000);

            assertFalse(disableRenewal.isAlive());
            assertEquals(1, loginCounter.get());
        } finally {
            provider.releaseRefreshCallback();
            provider.close();
        }
    }

    @Test
    public void testOAuthLogsExcludeSensitiveValues() throws Exception {
        final TestLogHandler handler = new TestLogHandler();
        final Logger testLogger = createLogger(handler);
        final String responseSecret = "REMOTE_RESPONSE_SECRET";

        loginStatus = HttpURLConnection.HTTP_UNAVAILABLE;
        loginErrorBody = responseSecret;
        TestProvider failedLogin = new TestProvider();
        failedLogin.setEndpoint(endpoint).setLogger(testLogger);
        try {
            failedLogin.getAuthorizationString(null);
            fail("The OAuth login should have failed");
        } catch (InvalidAuthorizationException expected) {
            assertFalse(expected.getMessage().contains(responseSecret));
        } finally {
            failedLogin.close();
        }
        assertEquals(1, loginCounter.get());
        assertLogExcludes(handler, oauthAccessToken, responseSecret);

        resetTestState();
        logoutStatus = HttpURLConnection.HTTP_INTERNAL_ERROR;
        logoutErrorBody = responseSecret;
        TestProvider failedLogout = new TestProvider();
        failedLogout.setEndpoint(endpoint)
                    .setLogger(testLogger)
                    .setAutoRenew(false);
        assertNotNull(failedLogout.getAuthorizationString(null));
        failedLogout.close();
        assertLogExcludes(handler, loginToken, responseSecret);

        resetTestState();
        loginTokenLifetimeMs = 12_000;
        final String callbackSecret = "CALLBACK_EXCEPTION_SECRET";
        FailingRefreshProvider failedRefresh =
            new FailingRefreshProvider(callbackSecret);
        failedRefresh.setEndpoint(endpoint).setLogger(testLogger);
        try {
            assertNotNull(failedRefresh.getAuthorizationString(null));
            failedRefresh.waitForRefreshAttempt(5_000);
        } finally {
            failedRefresh.close();
        }
        assertLogExcludes(handler, oauthAccessToken, callbackSecret);
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
        try (OutputStream os = exchange.getResponseBody()) {
            long expireTime =
                System.currentTimeMillis() + loginTokenLifetimeMs;
            final String jsonString = createLoginResponse(
                encodeLoginToken(tokenText, expireTime), expireTime,
                issuer, subjectType, subjectId);

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK,
                                         jsonString.length());
            os.write(jsonString.getBytes());
            os.flush();
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to encode", ioe);
        }
    }

    private static String encodeLoginToken(String tokenText, long expireAt) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeShort(1);
            oos.writeLong(expireAt);
            oos.writeBytes(tokenText);
            oos.flush();
            return JsonUtils.convertBytesToHex(baos.toByteArray());
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to encode", ioe);
        }
    }

    private static String createLoginResponse(String encodedToken,
                                              long expireAt,
                                              String issuer,
                                              String subjectType,
                                              String subjectId) {
        return "{\"token\":\"" + encodedToken + "\"," +
               "\"expireAt\":" + expireAt +
               (issuer != null ?
                   ",\"authenticatedIdentity\":{" +
                   "\"type\":\"oauth\"," +
                   "\"issuer\":\"" + issuer + "\"," +
                   "\"subjectType\":\"" + subjectType + "\"," +
                   "\"subjectId\":\"" + subjectId + "\"}" : "") +
               "}";
    }

    private static void sendResponse(HttpExchange exchange,
                                     int status,
                                     String body)
        throws IOException {

        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            if (bytes.length > 0) {
                os.write(bytes);
            }
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

    private static void waitForAutoRenew(OAuthAccessTokenProvider provider,
                                         boolean expected,
                                         long timeoutMs)
        throws InterruptedException {

        final long limit = System.currentTimeMillis() + timeoutMs;
        while (provider.isAutoRenew() != expected &&
               System.currentTimeMillis() < limit) {
            Thread.sleep(10);
        }
        assertEquals(expected, provider.isAutoRenew());
    }

    private static ScheduledThreadPoolExecutor getScheduler(
        OAuthAccessTokenProvider provider)
        throws Exception {

        final Field field =
            OAuthAccessTokenProvider.class.getDeclaredField("scheduler");
        field.setAccessible(true);
        return (ScheduledThreadPoolExecutor) field.get(provider);
    }

    private static Object getProviderField(OAuthAccessTokenProvider provider,
                                           String fieldName)
        throws Exception {

        final Field field =
            OAuthAccessTokenProvider.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(provider);
    }

    private static Logger createLogger(Handler handler) {
        final Logger logger = Logger.getLogger(
            OAuthAccessTokenProviderTest.class.getName() + "." +
            System.nanoTime());
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
        return logger;
    }

    private static void assertLogExcludes(TestLogHandler handler,
                                          String... excludedValues) {
        final String messages = handler.getMessages();
        for (String value : excludedValues) {
            assertFalse("Log contains sensitive value: " + value,
                        messages.contains(value));
        }
    }

    private static class TestProvider extends OAuthAccessTokenProvider {

        private final AtomicInteger tokenCounter = new AtomicInteger();

        @Override
        protected String getAccessToken() {
            if (tokenCounter.incrementAndGet() == 1) {
                return oauthAccessToken;
            }
            return secondOAuthAccessToken;
        }
    }

    private static class ClosingCallbackProvider
        extends OAuthAccessTokenProvider {

        @Override
        protected String getAccessToken() {
            close();
            return oauthAccessToken;
        }
    }

    private static class BlockingRefreshProvider
        extends OAuthAccessTokenProvider {

        private final AtomicInteger callbackCount = new AtomicInteger();
        private final CountDownLatch refreshCallback = new CountDownLatch(1);
        private final CountDownLatch releaseRefresh = new CountDownLatch(1);

        @Override
        protected String getAccessToken() {
            if (callbackCount.incrementAndGet() == 1) {
                return oauthAccessToken;
            }
            refreshCallback.countDown();
            try {
                if (!releaseRefresh.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                        "Timed out waiting to release refresh callback");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                    "Refresh callback interrupted", ie);
            }
            return secondOAuthAccessToken;
        }

        private boolean awaitRefreshCallback(long timeoutMs)
            throws InterruptedException {

            return refreshCallback.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        private void releaseRefreshCallback() {
            releaseRefresh.countDown();
        }
    }

    private static class FailingRefreshProvider
        extends OAuthAccessTokenProvider {

        private final AtomicInteger tokenCounter = new AtomicInteger();
        private final String failureMessage;

        FailingRefreshProvider() {
            this("test refresh failure");
        }

        FailingRefreshProvider(String failureMessage) {
            this.failureMessage = failureMessage;
        }

        @Override
        protected String getAccessToken() {
            if (tokenCounter.incrementAndGet() == 1) {
                return oauthAccessToken;
            }
            throw new IllegalStateException(failureMessage);
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

    private static class TestLogHandler extends Handler {

        private final StringBuilder messages = new StringBuilder();

        @Override
        public synchronized void publish(LogRecord record) {
            if (isLoggable(record)) {
                messages.append(record.getMessage()).append('\n');
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        private synchronized String getMessages() {
            return messages.toString();
        }
    }
}
