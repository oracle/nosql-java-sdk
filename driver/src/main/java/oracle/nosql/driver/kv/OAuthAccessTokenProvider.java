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
import java.util.concurrent.ScheduledFuture;
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
    private final AtomicReference<String> authString =
        new AtomicReference<String>();

    /*
     * Access token and its lifetime
     */
    private AccessTokenInfo tokenInfo;

    /*
     * KV-authenticated principal associated with this provider's login token.
     */
    private String loginPrincipal;

    /* Default refresh time before AT expiry, 10 seconds */
    private static final int REFRESH_AHEAD_SECONDS = 10;

    /*
     * logger
     */
    private Logger logger;

    /*
     * Whether to renew the login token automatically
     */
    private volatile boolean autoRenew = true;

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
    private volatile boolean isClosed = false;

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

    /*
     * Current scheduled refresh task.
     */
    private ScheduledFuture<?> refreshTask;


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

    private synchronized void performLogin(boolean force) {
        /* re-check the authString in case of a race */
        if (isClosed || (!force && authString.get() != null)) {
            return;
        }

        tokenInfo = validateAccessTokenInfo(getAccessTokenInfo());

        try {
            /*
             * Send request to server for login token
             */
            HttpResponse response =
                sendRequest(BEARER_PREFIX + tokenInfo.getAccessToken(),
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
            final LoginResult loginResult =
                parseJsonResult(response.getOutput());
            try {
                validateLoginPrincipal(loginResult.getPrincipal());
            } catch (InvalidAuthorizationException iae) {
                final String rejectedToken = loginResult.getToken();
                if (rejectedToken != null && !rejectedToken.isEmpty()) {
                    logoutSession(BEARER_PREFIX + rejectedToken);
                }
                throw iae;
            }
            authString.set(BEARER_PREFIX + loginResult.getToken());
            /*
             * Schedule access token refresh thread
             */
            scheduleRefresh();

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
            performLogin(false);
        }
        return authString.get();
    }

    /**
     * Closes the provider, releasing resources such as a stored login token.
     */
    @Override
    public synchronized void close() {

        /*
         * Already closed
         */
        if (isClosed) {
            return;
        }

        final String logoutAuth = authString.get();
        isClosed = true;
        if (!scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        if (refreshTask != null) {
            refreshTask.cancel(false);
            refreshTask = null;
        }

        /*
         * Send request for logout
         */
        if (logoutAuth != null) {
            logoutSession(logoutAuth);
        }

        /*
         * Clean up
         */
        authString.set(null);
        tokenInfo = null;
        loginPrincipal = null;
    }

    private void logoutSession(String logoutAuth) {
        try {
            final HttpResponse response =
                sendRequest(logoutAuth, LOGOUT_SERVICE);
            if (response.getStatusCode() != HttpResponseStatus.OK.code() &&
                logger != null) {
                logger.info("Failed to logout OAuth session, response: " +
                            response.getOutput());
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to logout OAuth session, exception: " + e);
            }
        }
    }

    /**
     * Invalidate the cached NoSQL login token.
     */
    @Override
    public void flushCache() {
        if (isClosed) {
            return;
        }
        authString.set(null);
    }

    private AccessTokenInfo validateAccessTokenInfo(
        AccessTokenInfo accessTokenInfo) {

        if (accessTokenInfo == null ||
            accessTokenInfo.getAccessToken() == null ||
            accessTokenInfo.getAccessToken().isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid access token provided");
        }
        return accessTokenInfo;
    }

    /**
     * Retrieve login token from JSON string.
     */
    private LoginResult parseJsonResult(String jsonResult) {
        final MapValue mapValue =
            JsonUtils.createValueFromJson(jsonResult, null).asMap();

        /*
         * Extract login token and authenticated principal from JSON result.
         */
        return new LoginResult(
            mapValue.getString("token"),
            mapValue.contains("principal") ?
                mapValue.getString("principal") : null);
    }

    private void validateLoginPrincipal(String principal) {
        if (principal == null || principal.isEmpty()) {
            throw new InvalidAuthorizationException(
                "Invalid OAuth login response: principal is missing");
        }
        if (loginPrincipal == null) {
            loginPrincipal = principal;
            return;
        }
        if (!loginPrincipal.equals(principal)) {
            throw new InvalidAuthorizationException(
                "Logout required prior to logging in with new user identity.");
        }
    }

    /* Schedule automatic re-login slightly before expiry */
    private synchronized void scheduleRefresh() {
        if (refreshTask != null) {
            refreshTask.cancel(false);
            refreshTask = null;
        }
        if (!autoRenew || isClosed || tokenInfo == null ||
            tokenInfo.getExpiresInSeconds() <= 0 || scheduler.isShutdown()) {
            return;
        }
        long delay = Math.max(1000,
            (tokenInfo.getExpiresInSeconds() - REFRESH_AHEAD_SECONDS) * 1000);
        refreshTask = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                refreshLoginToken();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void refreshLoginToken() {
        if (!autoRenew || isClosed) {
            return;
        }

        try {
            performLogin(true);
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to obtain refreshed token: " + e);
            }
            flushCache();
        }
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
        URL url = NoSQLHandleConfig.createURL(endpoint, "");
        if (!url.getProtocol().toLowerCase().equals("https")) {
            throw new IllegalArgumentException(
                "OAuthAccessTokenProvider requires use of https");
        }
        final String newLoginHost = url.getHost();
        final int newLoginPort = url.getPort();

        this.endpoint = endpoint;
        this.loginHost = newLoginHost;
        this.loginPort = newLoginPort;
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
     * Returns whether the login token is to be automatically renewed.
     *
     * @return true if auto-renew is set
     */
    public boolean isAutoRenew() {
        return autoRenew;
    }

    /**
     * Sets the auto-renew state. If true, automatic renewal of the login
     * token is enabled.
     *
     * @param autoRenew set to true to enable auto-renew
     *
     * @return this
     */
    public OAuthAccessTokenProvider setAutoRenew(boolean autoRenew) {
        this.autoRenew = autoRenew;
        return this;
    }

    /**
     * Send HTTPS request to login/logout service location with proper
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
        private final long expiresInSeconds;

        /**
         * Creates access token information.
         *
         * @param accessToken OAuth access token
         * @param expiresInSeconds token lifetime in seconds
         */
        public AccessTokenInfo(String accessToken, long expiresInSeconds) {
            if (expiresInSeconds < 0) {
                throw new IllegalArgumentException(
                    "Access token lifetime must be non-negative");
            }
            this.accessToken = accessToken;
            this.expiresInSeconds = expiresInSeconds;
        }

        public String getAccessToken() {
            return accessToken;
        }

        /**
         * Returns the access token lifetime in seconds.
         *
         * @return the access token lifetime in seconds
         */
        public long getExpiresInSeconds() {
            return expiresInSeconds;
        }

    }

    private static final class LoginResult {

        private final String token;
        private final String principal;

        private LoginResult(String token, String principal) {
            this.token = token;
            this.principal = principal;
        }

        private String getToken() {
            return token;
        }

        private String getPrincipal() {
            return principal;
        }
    }
}
