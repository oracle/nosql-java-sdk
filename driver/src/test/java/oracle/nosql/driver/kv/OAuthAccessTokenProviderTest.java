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
    private static final String logoutPath = KV_SECURITY_PATH + "/oauthlogout";

    private static final int port = 1444;
    private static final String endpoint = "https://localhost:" + port;

    private static final String oauthAccessToken = "OCI_ACCESS_TOKEN";
    private static final String secondOAuthAccessToken = "OCI_ACCESS_TOKEN_2";
    private static final String loginToken = "OAUTH_LOGIN_TOKEN";
    private static final String reloginToken = "OAUTH_RELOGIN_TOKEN";
    private static final String loginPrincipal = "oauth-data/it@test.com";
    private static final String differentLoginPrincipal =
        "oauth-data/other@test.com";
    private static final String authTokenPrefix = "Bearer ";

    private static HttpServer server;
    private static final AtomicInteger loginCounter = new AtomicInteger();
    private static final AtomicInteger logoutCounter = new AtomicInteger();
    private static volatile String lastLogoutToken;
    private static volatile String reloginPrincipal = loginPrincipal;
    private static volatile boolean omitLoginPrincipal;

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
                    generateLoginToken(
                        loginToken,
                        omitLoginPrincipal ? null : loginPrincipal,
                        exchange);
                } else {
                    assertEquals(authTokenPrefix + secondOAuthAccessToken,
                                 authString);
                    generateLoginToken(reloginToken, reloginPrincipal,
                                       exchange);
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
        omitLoginPrincipal = false;
        reloginPrincipal = loginPrincipal;
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
        omitLoginPrincipal = false;
        reloginPrincipal = loginPrincipal;
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
    public void testFlushCacheRelogin() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        omitLoginPrincipal = false;
        reloginPrincipal = loginPrincipal;
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
    public void testReloginWithDifferentPrincipalFails() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        omitLoginPrincipal = false;
        reloginPrincipal = differentLoginPrincipal;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            final String authString = provider.getAuthorizationString(null);
            assertEquals(loginToken, readTokenFromAuth(authString));

            provider.flushCache();

            provider.getAuthorizationString(null);
            fail("Relogin with a different principal should have failed");
        } catch (InvalidAuthorizationException iae) {
            assertTrue(iae.getMessage().startsWith(
                "Logout required prior to logging in with new " +
                "user identity."));
        } finally {
            reloginPrincipal = loginPrincipal;
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(reloginToken, lastLogoutToken);
    }

    @Test
    public void testLoginWithoutPrincipalFails() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        lastLogoutToken = null;
        omitLoginPrincipal = true;
        reloginPrincipal = loginPrincipal;
        TestProvider provider = new TestProvider();
        provider.setEndpoint(endpoint).setAutoRenew(false);

        try {
            provider.getAuthorizationString(null);
            fail("Login without a principal should have failed");
        } catch (InvalidAuthorizationException iae) {
            assertTrue(iae.getMessage().startsWith(
                "Invalid OAuth login response: principal is missing"));
        } finally {
            omitLoginPrincipal = false;
            provider.close();
        }
        assertEquals(1, logoutCounter.get());
        assertEquals(loginToken, lastLogoutToken);
    }

    @Test
    public void testCloseLogsOutLoginToken() throws Exception {
        loginCounter.set(0);
        logoutCounter.set(0);
        omitLoginPrincipal = false;
        reloginPrincipal = loginPrincipal;
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

    private static void generateLoginToken(String tokenText,
                                           String principal,
                                           HttpExchange exchange) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos);
             OutputStream os = exchange.getResponseBody()) {

            long expireTime = System.currentTimeMillis() + 15000;
            oos.writeShort(1);
            oos.writeLong(expireTime);
            oos.writeBytes(tokenText);
            oos.flush();

            final String tokenString =
                JsonUtils.convertBytesToHex(baos.toByteArray());
            final String jsonString =
                "{\"token\":\"" + tokenString + "\"," +
                "\"expireAt\":" + expireTime +
                (principal != null ?
                    ",\"principal\":\"" + principal + "\"" : "") +
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

    private static class TestProvider extends OAuthAccessTokenProvider {

        private final AtomicInteger tokenCounter = new AtomicInteger();

        @Override
        protected AccessTokenInfo getAccessTokenInfo() {
            if (tokenCounter.incrementAndGet() == 1) {
                return new AccessTokenInfo(oauthAccessToken, 15);
            }
            return new AccessTokenInfo(secondOAuthAccessToken, 15);
        }
    }
}
