/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import static oracle.nosql.driver.util.HttpConstants.SERVER_WARNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.Test;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.Request;

public class ClientWarningTest {

    @Test
    public void proxyWarningIsLoggedOncePerMessage() {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        CapturingHandler handler = new CapturingHandler();
        logger.addHandler(handler);

        Client client = new TestClient(logger, config());
        try {
            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add(SERVER_WARNING, "Upgrade the SDK");
            processResponse(client, headers);
            processResponse(client, headers);

            headers.set(SERVER_WARNING, "A different proxy warning");
            processResponse(client, headers);

            assertEquals(2, handler.messages.size());
            assertEquals("Upgrade the SDK", handler.messages.get(0));
            assertEquals("A different proxy warning", handler.messages.get(1));
        } finally {
            client.shutdown();
            logger.removeHandler(handler);
        }
    }

    private void processResponse(Client client, DefaultHttpHeaders headers) {
        assertThrows(NoSQLException.class,
                     () -> client.processResponse(
                         HttpResponseStatus.INTERNAL_SERVER_ERROR,
                         headers, Unpooled.EMPTY_BUFFER, null,
                         (short) 0, (short) 0));
    }

    private NoSQLHandleConfig config() {
        NoSQLHandleConfig config =
            new NoSQLHandleConfig("http://localhost:8080");
        config.setAuthorizationProvider(new AuthorizationProvider() {
            @Override
            public String getAuthorizationString(Request request) {
                return "test";
            }

            @Override
            public void close() {
            }
        });
        return config;
    }

    private static class TestClient extends Client {
        TestClient(Logger logger, NoSQLHandleConfig config) {
            super(logger, config);
        }

        @Override
        public HttpClient createHttpClient(URL url,
                                           NoSQLHandleConfig config,
                                           SslContext sslCtx,
                                           Logger logger) {
            return new HttpClient(url.getHost(), url.getPort(), 1,
                                  -1, 0, 0, 0, null, 0,
                                  "ClientWarningTest", logger);
        }
    }

    private static class CapturingHandler extends Handler {
        private final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
                messages.add(record.getMessage());
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
