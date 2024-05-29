/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.kv;

import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.KV_SECURITY_PATH;

import java.net.URL;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.httpclient.HttpResponse;
import oracle.nosql.driver.httpclient.ReactorHttpClient;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * On-premises only.
 * <p>
 * StoreAccessTokenProvider is an {@link AuthorizationProvider} that performs
 * the following functions:
 * <ul>
 * <li>Initial (bootstrap) login to store, using credentials provided</li>
 * <li>Storage of bootstrap login token for re-use</li>
 * <li>Optionally renews the login token before it expires</li>
 * <li>Logs out of the store when closed</li>
 * <li>User must call prepare() to boot strap login before using</li>
 * </ul>
 * <p>
 * If accessing an insecure instance of Oracle NoSQL Database the default
 * constructor is used, with no arguments.
 * <p>
 * If accessing a secure instance of Oracle NoSQL Database a user name and
 * password must be provided. That user must already exist in the NoSQL
 * Database and have sufficient permission to perform table operations.
 * That user's identity is used to authorize all database operations.
 */
public class StoreAccessTokenProvider implements AuthorizationProvider {

    /*
     * This is used when we send user:password pair.
     */
    private static final String BASIC_PREFIX = "Basic ";

    /*
     * This is the general prefix for the login token.
     */
    private static final String BEARER_PREFIX = "Bearer ";

    /*
     * login service end point name.
     */
    private static final String LOGIN_SERVICE = "/login";

    /*
     * login token renew service end point name.
     */
    private static final String RENEW_SERVICE = "/renew";

    /*
     * logout service end point name.
     */
    private static final String LOGOUT_SERVICE = "/logout";

    /*
     * Default timeout when sending http request to server
     */
    private static final int HTTP_TIMEOUT_MS = 30000;

    /*
     * Authentication token which contain the Bearer prefix and login token's
     * binary representation in hex format and token expiration.
     */
    private final AtomicReference<Token> authToken = new AtomicReference<>();

    /* Scheduled Refresh task */
    private final AtomicReference<Disposable> refreshTask =
            new AtomicReference<>();

    /* A Flag to indicate whether signature flush is in progress */
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    /* Thread to run refresh task */
    private final Scheduler refreshScheduler = Schedulers.newSingle("token" +
            "-refresh", true);
    /*
     * logger
     */
    private Logger logger;

    /*
     * Whether to renew the login token automatically
     */
    private volatile boolean autoRenew = true;

    /*
     * Whether this is a secure store token provider.
     */
    private final boolean isSecure;

    /*
     * Name of the user to login
     */
    private final String userName;

    /*
     * Password of the user to login
     */
    private final char[] password;

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


    /**
     * This method is used for access to a store without security enabled.
     */
    public StoreAccessTokenProvider() {
        isSecure = false;
        userName = null;
        password = null;
        loginHost = null;
        endpoint = null;
        loginPort = 0;
    }

    /**
     * This constructor is used when accessing a secure store.
     * <p>
     * This constructor requires a valid user name and password to access
     * the target store. The user must exist and have sufficient permission
     * to perform table operations required by the application. The user
     * identity is used to authorize all operations performed by the
     * application.
     *
     * @param userName the user name to use for the store. This user must
     * exist in the NoSQL Database and is the identity that is used for
     * authorizing all database operations.
     * @param password the password for the user.
     *
     * @throws IllegalArgumentException if one or more of the parameters is
     * malformed or a required parameter is missing.
     */
    public StoreAccessTokenProvider(String userName,
                                    char[] password) {

        /*
         * Check null
         */
        if (userName == null || userName.isEmpty() ||
                password == null) {
            throw new IllegalArgumentException(
                    "Invalid input arguments");
        }
        isSecure = true;
        this.userName = userName;
        this.password = Arrays.copyOf(password, password.length);
        this.logger = null;
    }

    /**
     * @hidden
     *
     * Bootstrap login using the provided credentials
     */
    private Mono<Token> bootstrapLogin() {
        if (!isSecure || isClosed) {
           return Mono.empty();
        }

        /*
         * Convert the user:password pair in base 64 format with
         * Basic prefix
         */
        final String encoded = Base64.getEncoder().
                encodeToString((
                    userName + ":" + String.valueOf(password)).getBytes());

        /*
         * Send request to server for login token
         */
        return getTokenFromServer(BASIC_PREFIX + encoded,
                LOGIN_SERVICE);
    }

    /**
     * @hidden
     */
    @Override
    public String getAuthorizationString(Request request) {
        return getAuthorizationStringAsync(request).block();
    }

    @Override
    public Mono<String> getAuthorizationStringAsync(Request request) {
        if (!isSecure || isClosed) {
            return Mono.empty();
        }
        return Mono.just(authToken.get().authString);
    }

    /**
     * @hidden
     */
    @Override
    public void validateAuthString(String input) {
        if (isSecure() && input == null) {
            throw new IllegalArgumentException(
                "Secured StoreAccessProvider acquired an unexpected null " +
                "authorization string");
        }
    }

    /**
     * Closes the provider, releasing resources such as a stored login
     * token.
     */
    @Override
    public synchronized void close() {

        /*
         * Don't do anything for non-secure case
         */
        if (!isSecure || isClosed) {
            return;
        }

        /*
         * Send request for logout
         */
        try {
            final HttpResponse response =
                    sendRequest(authToken.get().authString,
            LOGOUT_SERVICE).block();
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                if (logger != null) {
                    logger.info("Failed to logout user " + userName +
                                ": " + response.getBodyAsString());
                }
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.info("Failed to logout user " + userName +
                            ": " + e);
            }
        }

        /*
         * Clean up
         */
        isClosed = true;
        authToken.set(null);
        Arrays.fill(password, ' ');
        if (refreshTask.get() != null) {
            refreshTask.get().dispose();
            refreshTask.set(null);
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
    public StoreAccessTokenProvider setLogger(Logger logger) {
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
    public StoreAccessTokenProvider setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        URL url = NoSQLHandleConfig.createURL(endpoint, "");
        if (!url.getProtocol().toLowerCase().equals("https") && isSecure &&
                !disableSSLHook) {
            throw new IllegalArgumentException(
                "StoreAccessTokenProvider requires use of https");
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
    public StoreAccessTokenProvider setSslContext(SslContext sslCtx) {
        this.sslContext = sslCtx;
        return this;
    }

    /**
     * Sets the SSL handshake timeout in milliseconds
     * @param timeoutMs the timeout in milliseconds
     * @return this
     */
    public StoreAccessTokenProvider setSslHandshakeTimeout(int timeoutMs) {
        this.sslHandshakeTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Returns whether the provider is accessing a secured store
     *
     * @return true if accessing a secure store
     */
    public boolean isSecure() {
        return isSecure;
    }

    /**
     * Schedule a login token renew when half of the token life time is
     * reached.
     */
    private void scheduleRefresh() {
        if (logger != null) {
            logger.fine("Scheduling token refresh");
        }
        if (!isSecure || !autoRenew || isClosed) {
            return;
        }
        // Get the existing token
        Token existingToken = authToken.get();
        Disposable task = refreshTask.getAndSet(null);
        if (task != null) {
            if (logger != null) {
                logger.fine("disposing refresh task");
            }
            task.dispose();
        }

        final long acquireTime = System.currentTimeMillis();
        if (existingToken.expirationTime <= 0) {
            return;
        }
        if (existingToken.expirationTime > acquireTime + 10000) {
            final long renewTime =
                    acquireTime + (existingToken.expirationTime - acquireTime) / 2;

            /* Attempt a renewal at the token half-life */
            task = refreshScheduler.schedule(
                () -> getTokenFromServer(existingToken.authString, RENEW_SERVICE)
                        .subscribe(),
                (renewTime - acquireTime),
                TimeUnit.MILLISECONDS
            );
            refreshTask.set(task);
        }
    }

    /**
     * Retrieve login token from JSON string
     */
    private Token parseJsonResult(String jsonResult) {
        final MapValue mapValue =
            JsonUtils.createValueFromJson(jsonResult, null).asMap();

        /*
         * Extract login token and expiration time from JSON result
         */
        String token = BEARER_PREFIX + mapValue.getString("token");
        long life = mapValue.getLong("expireAt");

        return new Token(token,life);
    }

    /**
     * Send HTTPS request to login/renew/logout service location with proper
     * authentication information.
     */
    private Mono<HttpResponse> sendRequest(String authHeader,
                                           String serviceName) {
        ReactorHttpClient client = null;
        try {
            final HttpHeaders headers = new DefaultHttpHeaders();
            headers.set(AUTHORIZATION, authHeader);
            client = ReactorHttpClient.createMinimalClient
                (loginHost,
                 loginPort,
                 (isSecure && !disableSSLHook) ? sslContext : null,
                 sslHandshakeTimeoutMs,
                 serviceName,
                 logger);
            String uri = NoSQLHandleConfig.createURL(endpoint,
                    basePath + serviceName).toString();

            return client.getRequest(uri , headers);
        } finally {
            if (client != null) {
                //client.shutdown();
            }
        }
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
    public StoreAccessTokenProvider setAutoRenew(boolean autoRenew) {
        this.autoRenew = autoRenew;
        return this;
    }

    public void prepare() {
        bootstrapLogin().block();
    }

    private Mono<Token> getTokenFromServer(String authHeader,
                                           String serviceName) {
        Mono<Token> tokenMono = sendRequest(authHeader,serviceName)
                .flatMap(response -> {
                    int responseCode = response.getStatusCode();
                    Mono<String> body = response.getBodyAsString();
                    return body.handle((bodyString, sink) -> {
                        if (responseCode != HttpResponseStatus.OK.code()) {
                            sink.error(new InvalidAuthorizationException(
                                    "Fail to login to service: " + bodyString));
                        }
                        sink.next(parseJsonResult(bodyString));
                    });
                });
        return tokenMono.doOnNext(token -> {
            authToken.set(token);
            scheduleRefresh();
        });
    }

    @Override
    public void flushCache() {
        if (!flushInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            Disposable task = refreshTask.getAndSet(null);
            if (task != null) {
                task.dispose();
            }
            bootstrapLogin().subscribe();
        } finally {
            flushInProgress.set(false);
        }
    }


    private static class Token {
        private final String authString;
        private final long expirationTime;

        public Token(String authString, long expirationTime) {
            this.authString = authString;
            this.expirationTime = expirationTime;
        }
    }
}
