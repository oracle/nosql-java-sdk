/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.kv;

import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.KV_SECURITY_PATH;

import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.HttpRequestUtil;
import oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

public abstract class OAuthAccessTokenProvider implements AuthorizationProvider {


    /*
     * This is the general prefix for the login token.
     */
    private static final String BEARER_PREFIX = "Bearer ";

    /*
     * login service end point name.
     */
    private static final String LOGIN_SERVICE = "/oauthlogin";

    /*
     * login token renew service end point name.
     */
    private static final String RENEW_SERVICE = "/oauthrenew";

    /*
     * logout service end point name.
     */
    private static final String LOGOUT_SERVICE = "/oauthlogout";

    /*
     * Default timeout when sending http request to server
     */
    private static final int HTTP_TIMEOUT_MS = 30000;

	/*
     * Authentication string which contain the Bearer prefix and login token's
     * binary representation in hex format.
     */
    private AtomicReference<String> authString = new AtomicReference<String>();

    /*
     * Access token and its lifetime
     */
    private AccessTokenInfo tokenInfo;

	/* Default refresh time before AT expiry, 10 seconds*/
    private static final int REFRESH_AHEAD_SECONDS = 10;

    /*
     * logger
     */
    private Logger logger;

    /*
     * Host name of the proxy machine which host the login service
     */
    private String loginHost;

    /*
     * Port number of the proxy machine which host the login service
     */
    private int loginPort;

    /*
     * Endpoint to reach the authenticating entity (Proxy)
     */
    private String endpoint;

    /*
     * Base path for security related services
     */
    private final static String basePath = KV_SECURITY_PATH;

    /*
     * Whether this provider is closed
     */
    private boolean isClosed = false;

    /*
     *  SslContext used by http client
     */
    private SslContext sslContext;

    /*
     * SSL handshake timeout in milliseconds;
     */
    private int sslHandshakeTimeoutMs;
    /**
     * @hidden
     * This is only used for unit test
     */
    public static boolean disableSSLHook;

    /*
     * A schedule used to periodically invoke the callback
     */
    private final ScheduledExecutorService scheduler;


    public OAuthAccessTokenProvider() {
        loginHost = null;
        endpoint = null;
        loginPort = 0;
        logger = null;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "OAuthTokenRefresher");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Returns an access token and its lifetime.
     * Implementations decide:
     *  - How to obtain it (cached, freshly requested, etc.)
     *  - How to refresh it when expired
     *  - Whether to store/retrieve refresh tokens
     */
    protected abstract AccessTokenInfo getAccessTokenInfo();

	/**
     * @hidden
     *
     * Login using the access token provided by the callback.
     */
	public synchronized void login() {
        /* re-check the authString in case of a race */
        if (isClosed || authString.get() != null) {
            return;
        }

        try {
            tokenInfo = getAccessTokenInfo();
            final String accessToken = tokenInfo.getAccessToken();
            if (accessToken == null || accessToken.isEmpty()) {
                throw new IllegalArgumentException("Invalid access token " +
                                                   "provided");
            }
            /*
            * Send request to server for login token
            */
            HttpResponse response = sendRequest(BEARER_PREFIX + accessToken,
                                                LOGIN_SERVICE);
            /*
            * login fail
            */
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                throw new InvalidAuthorizationException(
                    "Fail to login to service: " + response.getOutput());
            }

            if (isClosed) {
                return;
            }

            /*
            * Generate the authentication string using login token
            */
            authString.set(BEARER_PREFIX +
                           parseJsonResult(response.getOutput()));
            /*
            * Schedule access token refresh thread
            */
            if (tokenInfo.getExpiresIn() > 0) {
                scheduleRefresh();
            }

        } catch (InvalidAuthorizationException iae) {
            throw iae;
        } catch (Exception e) {
            throw new NoSQLException("Login with OAuth token failed", e);
        }
    }

    /**
     * @hidden
     */
    @Override
    public String getAuthorizationString(Request request) {

        /*
         * Already close
         */
        if (isClosed) {
            return null;
        }

        /*
         * If there is no cached auth string, re-authentication to retrieve
         * the login token and generate the auth string.
         */
		if (authString.get() == null) {
            login();
        }
		return authString.get();
	}

    /**
     * Closes the provider, releasing resources such as a stored login token.
     */
    @Override
    public void close() {

        /*
         * Already closed
         */
        if (isClosed) {
            return;
        }

        /*
         * Send request for logout
         */
        try {
            final HttpResponse response =
                sendRequest(authString.get(), LOGOUT_SERVICE);
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                if (logger != null) {
                    logger.info("Failed to logout OAuth session from token: " +
                                tokenInfo.getAccessToken() + ", response: " +
                                response.getOutput());
                }
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to logout OAuth session from token: " +
                            tokenInfo.getAccessToken() + ", exception: " + e);
            }
        }

        /*
         * Clean up
         */
        isClosed = true;
        authString = null;
        tokenInfo = null;
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

	/* Schedule automatic re-login slightly before expiry */
    private void scheduleRefresh() {
        long delay = Math.max(1000,
            (tokenInfo.getExpiresIn() - REFRESH_AHEAD_SECONDS) * 1000);
        scheduler.schedule(() -> {
            try {
                login();
            } catch (Exception e) {
                if (logger != null) {
                    logger.info("Failed to obtain refreshed token: " + e);
                }

                if (!scheduler.isShutdown()) {
                    scheduler.shutdown();
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the logger, or null if not set.
     *
     * @return the logger
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Sets a Logger instance for this provider.
     * @param logger the logger
     * @return this
     */
    public OAuthAccessTokenProvider setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    /**
     * Returns the endpoint of the authenticating entity
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the endpoint of the authenticating entity
     * @param endpoint the endpoint
     * @return this
     * @throws IllegalArgumentException if the endpoint is not correctly
     * formatted
     */
    public OAuthAccessTokenProvider setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        URL url = NoSQLHandleConfig.createURL(endpoint, "");
        if (!url.getProtocol().toLowerCase().equals("https")) {
            throw new IllegalArgumentException(
                "OAuthAccessTokenProvider requires use of https");
        }
        this.loginHost = url.getHost();
        this.loginPort = url.getPort();
        return this;
    }

    /**
     * Sets the SSL context
     * @param sslCtx the context
     * @return this
     */
    public OAuthAccessTokenProvider setSslContext(SslContext sslCtx) {
        this.sslContext = sslCtx;
        return this;
    }

    /**
     * Sets the SSL handshake timeout in milliseconds
     * @param timeoutMs the timeout in milliseconds
     * @return this
     */
    public OAuthAccessTokenProvider setSslHandshakeTimeout(int timeoutMs) {
        this.sslHandshakeTimeoutMs = timeoutMs;
        return this;
    }

        /**
     * Retrieve login token from JSON string
     */
    private String parseJsonResult(String jsonResult) {
        final MapValue mapValue =
            JsonUtils.createValueFromJson(jsonResult, null).asMap();

        /*
         * Extract login token from JSON result
         */
        return mapValue.getString("token");
    }

    /**
     * Send HTTPS request to login/renew/logout service location with proper
     * authentication information.
     */
    private HttpResponse sendRequest(String authHeader,
                                     String serviceName) throws Exception {
        HttpClient client = null;
        try {
            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.set(AUTHORIZATION, authHeader);
            client = HttpClient.createMinimalClient
                (loginHost,
                 loginPort,
                 !disableSSLHook ? sslContext : null,
                 sslHandshakeTimeoutMs,
                 serviceName,
                 logger);
            return HttpRequestUtil.doGetRequest(
                client,
                NoSQLHandleConfig.createURL(endpoint, basePath + serviceName)
                .toString(),
                headers, HTTP_TIMEOUT_MS, logger);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /** Nested static class to store the access token and its lifetime */
    public static final class AccessTokenInfo {

        private final String accessToken;
        private final long expiresIn;

        public AccessTokenInfo(String accessToken, long expiresIn) {
            this.accessToken = accessToken;
            this.expiresIn = expiresIn;
        }

        public String getAccessToken() {
            return accessToken;
        }
        public long getExpiresIn() {
            return expiresIn;
        }
    }
}
