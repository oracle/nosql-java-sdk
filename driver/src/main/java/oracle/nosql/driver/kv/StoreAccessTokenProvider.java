/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

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

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;

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
     * Authentication string which contain the Bearer prefix and login token's
     * binary representation in hex format.
     */
    private AtomicReference<String> authString = new AtomicReference<String>();

    /*
     * Login token expiration time.
     */
    private long expirationTime;

    /*
     * A timer task used to periodically renew the login token.
     */
    private volatile Timer timer;

    /*
     * logger
     */
    private Logger logger;

    /*
     * Whether to renew the login token automatically
     */
    private boolean autoRenew = true;

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

        isSecure = true;
        this.userName = userName;
        this.password = Arrays.copyOf(password, password.length);
        this.logger = null;

        /*
         * Check null
         */
        if (this.userName == null || this.userName.isEmpty() ||
            this.password == null) {
            throw new IllegalArgumentException(
                "Invalid input arguments");
        }
    }

    /**
     * @hidden
     *
     * Bootstrap login using the provided credentials
     */
    public synchronized void bootstrapLogin() {

        if (!isSecure || isClosed) {
            return;
        }

        try {
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
            HttpResponse response = sendRequest(BASIC_PREFIX + encoded,
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
             * Schedule login token renew thread
             */
            scheduleRefresh();

        } catch (InvalidAuthorizationException iae) {
            throw iae;
        } catch (Exception e) {
            throw new NoSQLException("Bootstrap login fail", e);
        }
    }

    /**
     * @hidden
     */
    @Override
    public String getAuthorizationString(Request request) {

        if (!isSecure) {
            return null;
        }

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
            bootstrapLogin();
        }
        return authString.get();
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
                sendRequest(authString.get(), LOGOUT_SERVICE);
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                if (logger != null) {
                    logger.info("Failed to logout user " + userName +
                                ": " + response.getOutput());
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
        authString = null;
        expirationTime = 0;
        Arrays.fill(password, ' ');
        if (timer != null) {
            timer.cancel();
            timer = null;
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
        if (!url.getProtocol().toLowerCase().equals("https") && isSecure) {
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
        this.sslHandshakeTimeoutMs = timeout;
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

        /*
         * Only run when autoRenew is set
         */
        if (!isSecure || !autoRenew) {
            return;
        }

        /*
         * Clean up any existing timer
         */
        if (timer != null) {
            timer.cancel();
            timer = null;
        }

        final long acquireTime = System.currentTimeMillis();

        if (expirationTime <= 0) {
            return;
        }

        /*
         * If it is 10 seconds before expiration, don't do further renew to
         * avoid to many renew request in the last few seconds.
         */
        if (expirationTime > acquireTime + 10000) {
            final long renewTime =
                acquireTime + (expirationTime - acquireTime) / 2;

            timer = new Timer(true /* isDaemon */);

            /* Attempt a renew at the token half-life */
            timer.schedule(new RefreshTask(), (renewTime - acquireTime));
        }
    }

    /**
     * Retrieve login token from JSON string
     */
    private String parseJsonResult(String jsonResult) {
        final MapValue mapValue =
            JsonUtils.createValueFromJson(jsonResult, null).asMap();

        /*
         * Extract expiration time from JSON result
         */
        expirationTime = mapValue.getLong("expireAt");

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
                 (isSecure && !disableSSLHook) ? sslContext : null,
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

    /**
     * This task sends a request to the server for login session extension.
     * Depending on the server policy, a new login token with new expiration
     * time may or may not be granted.
     */
    private class RefreshTask extends TimerTask {

        @Override
        public void run() {

            if (!isSecure || !autoRenew || isClosed) {
                return;
            }

            try {
                final String oldAuth = authString.get();
                HttpResponse response = sendRequest(oldAuth,
                                                    RENEW_SERVICE);
                final String token = parseJsonResult(response.getOutput());
                if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                    throw new InvalidAuthorizationException(token);
                }
                if (isClosed) {
                    return;
                }
                authString.compareAndSet(oldAuth,
                                         BEARER_PREFIX + token);

                scheduleRefresh();
            } catch (Exception e) {
                if (logger != null) {
                    logger.info("Failed to renew login token: " + e);
                }

                if (timer != null) {
                    timer.cancel();
                    timer = null;
                }
            }
        }
    }
}
