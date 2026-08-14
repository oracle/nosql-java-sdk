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
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.HttpRequestUtil;
import oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

/**
 * On-premises only.
 *
 * <p>An authorization provider that exchanges an application-supplied OAuth
 * access token for a NoSQL login token through an OAuth-enabled proxy. The
 * NoSQL login token is cached and used to authorize subsequent operations.</p>
 *
 * <p>Applications implement {@link #getAccessToken()} and remain responsible
 * for acquiring and maintaining OAuth tokens. By default, this provider calls
 * that method again and performs a new login shortly before the current NoSQL
 * login token expires. The server-returned expiration is bounded by both the
 * validated OAuth token expiration and the configured store session timeout.
 * Automatic re-login can be disabled with {@link #setAutoRenew(boolean)}.</p>
 *
 * <p>OAuth access tokens and NoSQL login tokens are bearer credentials, so an
 * HTTPS service endpoint is required.</p>
 */
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
     * Existing NoSQL login-token logout service. This does not revoke the
     * original OAuth access token at the identity provider.
     */
    private static final String LOGOUT_SERVICE = "/logout";

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
     * Expiration time of the NoSQL login token, in milliseconds since epoch.
     * The server caps this deadline at the earlier of the validated OAuth
     * access-token expiration and the configured store session timeout.
     */
    private volatile long loginTokenExpireAt;

    /*
     * KV-authenticated identity associated with this provider's login token.
     */
    private OAuthIdentity authenticatedIdentity;

    /* Default refresh time before NoSQL login-token expiry, 10 seconds */
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
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /*
     *  SslContext used by http client
     */
    private SslContext sslContext;

    /*
     * SSL handshake timeout in milliseconds;
     */
    private int sslHandshakeTimeoutMs;

    /* Handle configuration used to propagate HTTP proxy settings. */
    private NoSQLHandleConfig handleConfig;

    /**
     * @hidden
     * This is only used for unit test
     */
    public static boolean disableSSLHook;

    /*
     * A schedule used to periodically invoke the callback
     */
    private final ScheduledThreadPoolExecutor scheduler;

    /*
     * Current scheduled refresh task.
     */
    private ScheduledFuture<?> refreshTask;

    /* Guards auto-renew state transitions and refreshTask. */
    private final Object refreshLock = new Object();

    /* Invalidates a scheduled refresh that has already started running. */
    private final AtomicLong refreshGeneration = new AtomicLong();


    /**
     * Creates a provider with automatic re-login enabled.
     */
    public OAuthAccessTokenProvider() {
        loginHost = null;
        endpoint = null;
        loginPort = 0;
        logger = null;
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "OAuthTokenRefresher");
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
    }

    /**
     * Returns an OAuth access token for a login exchange.
     *
     * <p>This method is called for the initial login and each subsequent
     * re-login. Implementations are responsible for obtaining a token that is
     * usable when returned, including refreshing or replacing a cached token
     * when necessary. Implementations should avoid returning a token they know
     * is expired; the server performs the authoritative token validation. The
     * SDK schedules re-login from the NoSQL login-token expiration returned by
     * the server.</p>
     *
     * @return an OAuth access token
     */
    protected abstract String getAccessToken();

    private synchronized void performLogin(boolean force,
                                           Request request,
                                           long expectedGeneration) {
        final String oldAuthorization = authString.get();
        /* re-check the authString in case of a race */
        if (loginAborted(force, expectedGeneration)) {
            return;
        }
        String accessToken = getAccessToken();
        if (accessToken == null || accessToken.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid access token provided");
        }

        if (loginAborted(force, expectedGeneration)) {
            return;
        }
        final int timeoutMs = getRemainingTimeoutMs(request);

        try {
            /*
             * Send request to server for login token
             */
            if (loginAborted(force, expectedGeneration)) {
                return;
            }
            HttpResponse response =
                sendRequest(BEARER_PREFIX + accessToken,
                            LOGIN_SERVICE, timeoutMs);

            if (loginAborted(force, expectedGeneration)) {
                logoutLoginResponse(response, timeoutMs);
                return;
            }

            /*
             * login fail
             */
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                throw new InvalidAuthorizationException(
                    "OAuth login failed with HTTP status " +
                    response.getStatusCode());
            }

            /*
             * Generate the authentication string using login token
             */
            final LoginResult loginResult;
            try {
                loginResult = parseJsonResult(response.getOutput());
                final long expireAt = loginResult.getExpireAt();
                if (expireAt <= System.currentTimeMillis()) {
                    throw new InvalidAuthorizationException(
                        "OAuth login response contains an expired login token");
                }
                validateAuthenticatedIdentity(
                    loginResult.getAuthenticatedIdentity());
            } catch (InvalidAuthorizationException iae) {
                logoutLoginResponse(response, timeoutMs);
                throw iae;
            }
            if (loginAborted(force, expectedGeneration) ||
                !authString.compareAndSet(
                    oldAuthorization,
                    BEARER_PREFIX + loginResult.getToken())) {
                logoutSession(
                    BEARER_PREFIX + loginResult.getToken(), timeoutMs);
                return;
            }
            loginTokenExpireAt = loginResult.getExpireAt();
            /*
             * Schedule re-login using the server-authoritative session
             * expiration.
             */
            scheduleRefresh();

        } catch (InvalidAuthorizationException iae) {
            throw iae;
        } catch (Exception e) {
            throw new NoSQLException("Login with OAuth token failed", e);
        }
    }

    private boolean loginAborted(boolean force, long expectedGeneration) {
        return isClosed.get() ||
               (expectedGeneration >= 0 &&
                (expectedGeneration != refreshGeneration.get() ||
                 !autoRenew)) ||
               (!force && authString.get() != null);
    }

    private int getRemainingTimeoutMs(Request request) {
        if (request == null) {
            return 0;
        }

        final int timeoutMs = request.getTimeoutInternal();
        final long startNanos = request.getStartNanos();
        if (timeoutMs <= 0 || startNanos == 0) {
            return timeoutMs;
        }

        final long elapsedNanos = System.nanoTime() - startNanos;
        if (elapsedNanos <= 0) {
            return timeoutMs;
        }
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        if (elapsedMs >= timeoutMs) {
            throw new RequestTimeoutException(
                timeoutMs, "OAuth login exceeded the request timeout");
        }
        return timeoutMs - (int) elapsedMs;
    }

    /**
     * @hidden
     */
    @Override
    public String getAuthorizationString(Request request) {

        /*
         * Already close
         */
        if (isClosed.get()) {
            return null;
        }

        /*
         * If there is no cached auth string, re-authentication to retrieve
         * the login token and generate the auth string.
         */
        if (authString.get() == null) {
            performLogin(false, request, -1);
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
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }

        refreshGeneration.incrementAndGet();
        final String logoutAuth;
        synchronized (this) {
            logoutAuth = authString.getAndSet(null);
            synchronized (refreshLock) {
                if (!scheduler.isShutdown()) {
                    scheduler.shutdownNow();
                }
                cancelRefreshTaskLocked();
            }

            loginTokenExpireAt = 0;
            authenticatedIdentity = null;
        }

        if (logoutAuth != null) {
            logoutSession(logoutAuth, 0);
        }
    }

    private void logoutSession(String logoutAuth, int timeoutMs) {
        try {
            final HttpResponse response =
                sendRequest(logoutAuth, LOGOUT_SERVICE, timeoutMs);
            if (response.getStatusCode() != HttpResponseStatus.OK.code() &&
                logger != null) {
                logger.info("Failed to logout OAuth session, HTTP status " +
                            response.getStatusCode());
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to logout OAuth session, exception type " +
                            e.getClass().getName());
            }
        }
    }

    /**
     * Invalidate the cached NoSQL login token.
     */
    @Override
    public void flushCache() {
        refreshGeneration.incrementAndGet();
        synchronized (this) {
            if (isClosed.get()) {
                return;
            }
            authString.set(null);
            cancelRefreshTask();
            loginTokenExpireAt = 0;
        }
    }

    /**
     * Invalidates the cached login token only if it was used by the failed
     * request. A newer token installed by another request is preserved.
     *
     * @hidden
     *
     * @param failedAuthorization authorization value used by the failed request
     * @return true if the cached value was invalidated
     */
    public boolean invalidateAuthorizationString(String failedAuthorization) {
        if (isClosed.get() || failedAuthorization == null ||
            !failedAuthorization.equals(authString.get())) {
            return false;
        }

        refreshGeneration.incrementAndGet();
        synchronized (this) {
            if (isClosed.get()) {
                return false;
            }
            if (!authString.compareAndSet(failedAuthorization, null)) {
                if (authString.get() != null && loginTokenExpireAt > 0) {
                    scheduleRefresh();
                }
                return false;
            }
            cancelRefreshTask();
            loginTokenExpireAt = 0;
            return true;
        }
    }

    /**
     * Retrieve login token from JSON string.
     */
    private LoginResult parseJsonResult(String jsonResult) {
        try {
            final FieldValue result =
                JsonUtils.createValueFromJson(jsonResult, null);
            if (!result.isMap()) {
                throw new IllegalArgumentException("Expected JSON object");
            }
            final MapValue mapValue = result.asMap();

            /*
             * Extract login token, expiration, and authenticated identity from
             * JSON result. Do not use the coercive MapValue getters here; the
             * endpoint contract requires exact JSON types.
             */
            final String token = getRequiredString(mapValue, "token");
            final FieldValue expireAtValue = mapValue.get("expireAt");
            if (expireAtValue == null ||
                !(expireAtValue.isLong() || expireAtValue.isInteger())) {
                throw new IllegalArgumentException("Invalid expireAt");
            }
            return new LoginResult(
                token,
                expireAtValue.getLong(),
                parseAuthenticatedIdentity(mapValue));
        } catch (InvalidAuthorizationException iae) {
            throw iae;
        } catch (RuntimeException re) {
            throw new InvalidAuthorizationException(
                "Invalid OAuth login response");
        }
    }

    private void logoutLoginResponse(HttpResponse response, int timeoutMs) {
        if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
            return;
        }
        try {
            final MapValue loginResult =
                JsonUtils.createValueFromJson(
                    response.getOutput(), null).asMap();
            final FieldValue tokenValue = loginResult.get("token");
            if (tokenValue != null && tokenValue.isString() &&
                !tokenValue.getString().trim().isEmpty()) {
                final String token = tokenValue.getString();
                logoutSession(BEARER_PREFIX + token, timeoutMs);
            }
        } catch (RuntimeException re) {
            if (logger != null) {
                logger.info("Unable to clean up OAuth login response, " +
                            "exception type " + re.getClass().getName());
            }
        }
    }

    private OAuthIdentity parseAuthenticatedIdentity(MapValue loginResult) {
        final FieldValue identityValue =
            loginResult.get("authenticatedIdentity");
        if (identityValue == null) {
            return null;
        }
        try {
            if (!identityValue.isMap()) {
                throw new IllegalArgumentException("Expected identity object");
            }
            final MapValue identity = identityValue.asMap();
            return new OAuthIdentity(
                getRequiredString(identity, "type"),
                getRequiredString(identity, "issuer"),
                getRequiredString(identity, "subjectType"),
                getRequiredString(identity, "subjectId"));
        } catch (RuntimeException re) {
            throw new InvalidAuthorizationException(
                "Invalid OAuth login response: authenticated identity is " +
                "invalid");
        }
    }

    private static String getRequiredString(MapValue value, String fieldName) {
        final FieldValue field = value.get(fieldName);
        if (field == null || !field.isString() ||
            field.getString().trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Missing or invalid " + fieldName);
        }
        return field.getString();
    }

    private void validateAuthenticatedIdentity(OAuthIdentity identity) {
        if (identity == null) {
            throw new InvalidAuthorizationException(
                "Invalid OAuth login response: authenticated identity is " +
                "missing");
        }
        if (authenticatedIdentity == null) {
            authenticatedIdentity = identity;
            return;
        }
        if (!authenticatedIdentity.equals(identity)) {
            throw new InvalidAuthorizationException(
                "Logout required prior to logging in with new user identity.");
        }
    }

    /* Schedule automatic re-login slightly before session expiry. */
    private void scheduleRefresh() {
        synchronized (refreshLock) {
            scheduleRefreshLocked();
        }
    }

    private void scheduleRefreshLocked() {
        final long generation = refreshGeneration.incrementAndGet();
        cancelRefreshTaskLocked();
        if (!autoRenew || isClosed.get() || authString.get() == null ||
            loginTokenExpireAt <= 0 || scheduler.isShutdown()) {
            return;
        }
        final long now = System.currentTimeMillis();
        final long delay = Math.max(
            1000,
            loginTokenExpireAt - now -
            TimeUnit.SECONDS.toMillis(REFRESH_AHEAD_SECONDS));
        refreshTask = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                refreshLoginToken(generation);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void refreshLoginToken(long generation) {
        if (!autoRenew || isClosed.get() ||
            generation != refreshGeneration.get()) {
            return;
        }

        try {
            performLogin(true, null, generation);
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to obtain refreshed token, exception " +
                            "type " + e.getClass().getName());
            }
        }
    }

    private void cancelRefreshTask() {
        synchronized (refreshLock) {
            cancelRefreshTaskLocked();
        }
    }

    private void cancelRefreshTaskLocked() {
        if (refreshTask != null) {
            refreshTask.cancel(false);
            refreshTask = null;
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
     * Internal use only.
     * <p>
     * Completes handle-dependent configuration while preserving an explicitly
     * configured OAuth endpoint or SSL context.
     *
     * @param config the handle configuration
     * @return this
     * @hidden
     */
    public OAuthAccessTokenProvider prepare(NoSQLHandleConfig config) {
        final URL serviceURL = config.getServiceURL();
        if (serviceURL == null ||
            !"https".equalsIgnoreCase(serviceURL.getProtocol())) {
            throw new IllegalArgumentException(
                "OAuthAccessTokenProvider requires use of https for the " +
                "service endpoint");
        }

        if (endpoint == null) {
            String serviceEndpoint = serviceURL.toString();
            if (serviceEndpoint.endsWith("/")) {
                serviceEndpoint = serviceEndpoint.substring(
                    0, serviceEndpoint.length() - 1);
            }
            setEndpoint(serviceEndpoint);
        }
        if (sslContext == null) {
            sslContext = config.getSslContext();
        }
        if (sslHandshakeTimeoutMs == 0) {
            sslHandshakeTimeoutMs = config.getSSLHandshakeTimeout();
        }
        if (!disableSSLHook && sslContext == null) {
            throw new IllegalArgumentException(
                "OAuthAccessTokenProvider requires an SSL context");
        }
        handleConfig = config;
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
        synchronized (refreshLock) {
            if (this.autoRenew == autoRenew) {
                return this;
            }
            this.autoRenew = autoRenew;
            if (autoRenew) {
                scheduleRefreshLocked();
            } else {
                refreshGeneration.incrementAndGet();
                cancelRefreshTaskLocked();
            }
        }
        return this;
    }

    /**
     * Send HTTPS request to login/logout service location with proper
     * authentication information.
     */
    private HttpResponse sendRequest(String authHeader,
                                     String serviceName,
                                     int timeoutMs) throws Exception {
        HttpClient client = null;
        try {
            if (!disableSSLHook && sslContext == null) {
                throw new IllegalStateException(
                    "OAuthAccessTokenProvider requires an SSL context");
            }
            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.set(AUTHORIZATION, authHeader);
            client = HttpClient.createMinimalClient
                (loginHost,
                 loginPort,
                 !disableSSLHook ? sslContext : null,
                 sslHandshakeTimeoutMs,
                 serviceName,
                 null);
            if (handleConfig != null &&
                handleConfig.getProxyHost() != null) {
                client.configureProxy(handleConfig);
            }
            if (timeoutMs == 0) {
                timeoutMs = HTTP_TIMEOUT_MS;
            }
            return HttpRequestUtil.doGetRequestOnce(
                client,
                NoSQLHandleConfig.createURL(endpoint, basePath + serviceName)
                .toString(),
                headers, timeoutMs, null);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private static final class LoginResult {

        private final String token;
        private final long expireAt;
        private final OAuthIdentity authenticatedIdentity;

        private LoginResult(String token,
                            long expireAt,
                            OAuthIdentity authenticatedIdentity) {
            this.token = token;
            this.expireAt = expireAt;
            this.authenticatedIdentity = authenticatedIdentity;
        }

        private String getToken() {
            return token;
        }

        private OAuthIdentity getAuthenticatedIdentity() {
            return authenticatedIdentity;
        }

        private long getExpireAt() {
            return expireAt;
        }
    }

    /** Immutable identity returned by the OAuth login endpoint. */
    private static final class OAuthIdentity {

        private final String issuer;
        private final String subjectType;
        private final String subjectId;

        private OAuthIdentity(String type,
                              String issuer,
                              String subjectType,
                              String subjectId) {
            if (!"oauth".equals(type) || isBlank(issuer) ||
                !("user".equals(subjectType) ||
                  "client".equals(subjectType)) ||
                isBlank(subjectId)) {
                throw new IllegalArgumentException(
                    "Invalid OAuth authenticated identity");
            }
            this.issuer = issuer;
            this.subjectType = subjectType;
            this.subjectId = subjectId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof OAuthIdentity)) {
                return false;
            }
            final OAuthIdentity that = (OAuthIdentity) other;
            return issuer.equals(that.issuer) &&
                   subjectType.equals(that.subjectType) &&
                   subjectId.equals(that.subjectId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(issuer, subjectType, subjectId);
        }

        private static boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }
    }
}
