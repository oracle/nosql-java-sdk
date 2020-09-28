/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
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

import oracle.nosql.driver.values.JsonUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StoreAccessTokenProviderTest {

    private static final String loginPath = KV_SECURITY_PATH + "/login";

    private static final String renewPath = KV_SECURITY_PATH + "/renew";

    private static final String logoutPath = KV_SECURITY_PATH + "/logout";

    /*
     * Don't use a privileged port (443); it will fail on some machines
     */
    private static final int port = 1443;

    private static final String endpoint = "https://localhost:" + port;

    /*
     * basicAuthString matching user name test and password NoSql00__123456
     */
    private static final String userName = "test";
    private static final String password = "NoSql00__123456";
    private static final String basicAuthString =
        "Basic dGVzdDpOb1NxbDAwX18xMjM0NTY=";

    private static final String loginToken = "LOGIN_TOKEN";
    private static final String renewToken = "RENEW_TOKEN";
    private static final String authTokenPrefix = "Bearer ";

    private static HttpServer server;

    @BeforeClass
    public static void staticSetUp() throws Exception {

        StoreAccessTokenProvider.disableSSLHook = true;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.start();
        server.createContext(loginPath, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange)
                throws IOException {
                final String authString =
                    exchange.getRequestHeaders().get(
                        AUTHORIZATION).get(0);
                assertEquals(authString, basicAuthString);
                generateLoginToken(loginToken, exchange);
            }
        });
        server.createContext(renewPath, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange)
                throws IOException {
                final String authString =
                    exchange.getRequestHeaders().get(
                        AUTHORIZATION).get(0);
                assertTrue(authString.startsWith(authTokenPrefix));
                generateLoginToken(renewToken, exchange);
            }
        });
        server.createContext(logoutPath, new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange)
                throws IOException {
                final String authString =
                    exchange.getRequestHeaders().get(
                        AUTHORIZATION).get(0);
                assertTrue(authString.startsWith(authTokenPrefix));
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                exchange.close();
            }
        });
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        StoreAccessTokenProvider.disableSSLHook = false;
        server.stop(0);
    }

    @Test
    public void testBasic() throws Exception {

        StoreAccessTokenProvider sap =
            new StoreAccessTokenProvider(
                userName, password.toCharArray());
        sap.setEndpoint(endpoint);

        try {
            final String authString = sap.getAuthorizationString(null);
            assertNotNull(authString);
            assertTrue(authString.startsWith(authTokenPrefix));
            assertEquals(readTokenFromAuth(authString), loginToken);
            /*
             * Wait for the refresh to complete
             */
            Thread.sleep(10000);
            final String authRenewString = sap.getAuthorizationString(null);
            assertEquals(readTokenFromAuth(authRenewString), renewToken);
            sap.close();
            assertNull(sap.getAuthorizationString(null));
        } finally {
            sap.close();
        }

        /* bad endpoints */
        tryBadEndpoint("http://localhost");
        tryBadEndpoint("localhost:8080");
        tryBadEndpoint("foo://localhost");
    }

    @Test
    public void testMultiThreads() throws Exception {

        int numThreads = 5;
        Thread threads[];

        StoreAccessTokenProvider sap =
            new StoreAccessTokenProvider(
                userName, password.toCharArray());
        sap.setEndpoint(endpoint);
        Runnable run = new TestMultiThreads(sap);

        /*
         * Start numThreads threads to do bootstrap login calls on
         * the same SAP in parallel to test thread-safety. Each thread
         * does N calls then exits.
         */

        threads = new Thread[numThreads];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(run);
            threads[i].start();
        }

        /* wait for threads */
        for (int i = 0; i < 5; i++) {
            threads[i].join();
        }
    }

    private class TestMultiThreads implements Runnable {

        private final StoreAccessTokenProvider sap;

        TestMultiThreads(StoreAccessTokenProvider sap) {
            this.sap = sap;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 5; i++) {
                    sap.bootstrapLogin();
                }
            } finally {
                sap.close();
            }
        }
    }

    private void tryBadEndpoint(String ep) {
        /* bad endpoints */
        char[] pass = {'a'};

        try {
            StoreAccessTokenProvider sap =
                new StoreAccessTokenProvider("a", pass);
            sap.setEndpoint(ep);
            fail("Endpoint should have failed: " + ep);
        } catch (IllegalArgumentException iae) {
            // ok
        }
    }

    private static void generateLoginToken(String tokenText,
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
                   "\"expireAt\":" + expireTime + "}";

               exchange.sendResponseHeaders(
                   HttpURLConnection.HTTP_OK, jsonString.length());

               os.write(jsonString.getBytes());
               os.flush();
           } catch (IOException ioe) {
               throw new IllegalArgumentException("Unabled to encode",
                                                  ioe);
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
}
