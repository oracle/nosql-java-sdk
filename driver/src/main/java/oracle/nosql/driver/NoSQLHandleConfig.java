/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.SignatureProvider;
import io.netty.handler.ssl.SslContext;

/**
 * NoSQLHandleConfig groups parameters used to configure a {@link
 * NoSQLHandle}. It also provides a way to default common parameters for use by
 * {@link NoSQLHandle} methods. When creating a {@link NoSQLHandle} the
 * NoSQLHandleConfig instance is copied so modification operations on the
 * instance have no effect on existing handles which are immutable. Handle
 * state with default values can be overridden in individual operations.
 * <p>
 * Most of the configuration parameters are optional and have default
 * values if not specified. The only required configuration is the
 * service endpoint required by the constructor.
 */
public class NoSQLHandleConfig implements Cloneable {

    /**
     * The default value for request, and table request timeouts in
     * milliseconds, if not configured.
     */
    private static final int DEFAULT_TIMEOUT = 5000;
    private static final int DEFAULT_TABLE_REQ_TIMEOUT = 10000;

    /**
     * The default value for Consistency if not configured.
     */
    private static final Consistency DEFAULT_CONSISTENCY =
        Consistency.EVENTUAL;

    private static final Set<String> VALID_SSL_PROTOCOLS = new HashSet<>();
    static {
        VALID_SSL_PROTOCOLS.add("SSLv2");
        VALID_SSL_PROTOCOLS.add("SSLv3");
        VALID_SSL_PROTOCOLS.add("TLSv1");
        VALID_SSL_PROTOCOLS.add("TLSv1.1");
        VALID_SSL_PROTOCOLS.add("TLSv1.2");
        VALID_SSL_PROTOCOLS.add("TLSv1.3");
    }

    /**
     * Statistics configuration, optional.
     */
    public static final String STATS_PROFILE_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.profile";
    public static final String STATS_INTERVAL_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.interval";
    public static final String STATS_PRETTY_PRINT_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.pretty-print";
    public static final String STATS_ENABLE_LOG_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.enable-log";

    /* Statistics logging interval in seconds. Default 600 sec, ie. 10 min. */
    public static final int DEFAULT_STATS_INTERVAL = 600;
    public static final StatsControl.Profile DEFAULT_STATS_PROFILE =
        StatsControl.Profile.NONE;
    public static final boolean DEFAULT_STATS_PRETTY_PRINT = false;
    public static final boolean DEFAULT_ENABLE_LOG = true;


    /*
     * The url used to contact an HTTP proxy
     */
    private final URL serviceURL;

    /*
     * Cloud Service Only. The region will be accessed by the NoSQLHandle, which
     * will be used to build the service URL.
     */
    private Region region;

    /**
     * The default request timeout, used when a specific operation doesn't
     * set its own.
     */
    private int timeout;

    /**
     * The default table request timeout, used when a table operation doesn't
     * set its own.
     */
    private int tableRequestTimeout;

    /**
     * The default read consistency, used when a specific operation doesn't
     * set its own.
     */
    private Consistency consistency;

    /**
     * The minimum size (low water mark) to keep in the pool, and keep alive
     */
    private int connectionPoolMinSize;

    /**
     * The number of seconds to use to cause unused connections to be closed.
     * 0 means don't close unused connections, but they can still be closed
     * by the server side
     */
    private int connectionPoolInactivityPeriod;

    /**
     * The number of threads to configure for handling asynchronous netty
     * traffic. Defaults to nCPUs * 2
     */
    private int numThreads;

    /**
     * The maximum content associated with a request. Payloads that exceed
     * this value will result in an IllegalArgumentException.
     *
     * Default: 0 (use HttpClient default value, currently 32MB)
     */
    private int maxContentLength = 0;

    /**
     * The maximum size of http chunks.
     *
     * Default: 0 (use HttpClient default value, currently 64KB)
     */
    private int maxChunkSize = 0;

    /**
     * A RetryHandler, or null if not configured by the user.
     */
    private RetryHandler retryHandler;

    /**
     * An AuthorizationProvider, or null if not configured by the user.
     */
    private AuthorizationProvider authProvider;

    /**
     * SslContext. In production this may be set internally so not used here.
     * This is used for testing, at least.
     */
    private SslContext sslCtx;

    /**
     * The Logger used by the driver, or null if not configured by the user.
     */
    private Logger logger;

    /**
     * The cipher suites used by the driver, or null if not configured
     * by the user.
     */
    private List<String> ciphers;

    /**
     * The SSL protocols used by the driver, or null if not configured
     * by the user.
     */
    private List<String> protocols;

    /**
     * The size of cache used for storing SSLSession objects, or 0 if not
     * configured by the user.
     */
    private int sslSessionCacheSize = 0;

    /**
     * The timeout limit of SSLSession objects, or 0 if not configured by
     * the user.
     */
    private int sslSessionTimeout = 0;

    /**
     * The timeout limit of SSH handshake, or 0 if not configured by
     * the user.
     */
    private int sslHandshakeTimeoutMs = 0;

    /**
     * Cloud service only.
     *
     * The default compartment name or ocid, if set. This may be null
     * in which case the compartment used is either the root compartment for
     * the tenancy or the compartment specified in a request. Any
     * compartment specified in a request or table name overrides this
     * default.
     */
    private String compartment;

    /**
     * Enable rate limiting.
     * Cloud service only.
     */
    private boolean rateLimitingEnabled;

    /**
     * Default percentage of table limits to use in rate limiting.
     * Cloud service only.
     */
    private double defaultRateLimiterPercentage;

    /**
     * HTTP Proxy configuration, optional
     */
    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;

    /**
     * Statistics configuration, optional.
     */
    private int statsInterval = DEFAULT_STATS_INTERVAL;
    private StatsControl.Profile statsProfile = DEFAULT_STATS_PROFILE;
    private boolean statsPrettyPrint = DEFAULT_STATS_PRETTY_PRINT;
    private boolean statsEnableLog = DEFAULT_ENABLE_LOG;
    private StatsControl.StatsHandler statsHandler = null;

    /**
     * Hidden flag to control automatic auth refresh in cloud service
     */
    private boolean authRefresh;
    /**
     * Additional extension to user agent http header.
     */
    private String extensionUserAgent;

    /**
     * Specifies an endpoint or region id to use to connect to the Oracle
     * NoSQL Database Cloud Service or, if on-premise, the Oracle NoSQL
     * Database proxy server.
     * <p>
     * There is flexibility in how endpoints are specified. A fully specified
     * endpoint is of the format:
     * <pre>    http[s]://host:port</pre>
     * This interface accepts portions of a fully specified endpoint, including
     * a region id (see {@link Region}) string if using the Cloud service.
     * <p>
     * A valid endpoint is one of these:
     * <ol>
     * <li>region id string (cloud service only)</li>
     * <li>a string with the syntax [http[s]://]host[:port]</li>
     * </ol>
     * <p>
     * For example, these are valid endpoint arguments:
     * <ul>
     * <li>us-ashburn-1</li>
     * <li>nosql.us-ashburn-1.oci.oraclecloud.com</li>
     * <li>https://nosql.us-ashburn-1.oci.oraclecloud.com:443</li>
     * <li>localhost:8080</li>
     * <li>https://machine-hosting-proxy:443</li>
     * </ul>
     * <p>
     * If using the endpoint (vs region id) syntax, if the port is omitted,
     * the endpoint defaults to 443. If the protocol is omitted, the
     * endpoint uses https if the port is 443, and http in all other cases.
     * <p>
     * If using the Cloud Service see {@link Region} for information on
     * available regions. If using the Cloud Service the constructor,
     * {@link NoSQLHandleConfig#NoSQLHandleConfig(Region,AuthorizationProvider)},
     * is available rather than using a Region's id string.
     *
     * @param endpoint identifies a region id or server for use by the
     * NoSQLHandle. This is a required parameter.
     *
     * @throws IllegalArgumentException if the endpoint is null or malformed.
     */
    public NoSQLHandleConfig(String endpoint) {
        super();
        endpoint = checkRegionId(endpoint, null);
        this.serviceURL = createURL(endpoint, "/");
        setConfigFromEnvironment();
    }

    /**
     * Specifies an endpoint or region id to use to connect to the Oracle
     * NoSQL Database Cloud Service or, if on-premise, the Oracle NoSQL
     * Database proxy server. In addition an {@link AuthorizationProvider}
     * is specified.
     * <p>
     * There is flexibility in how endpoints are specified. A fully specified
     * endpoint is of the format:
     * <pre>    http[s]://host:port</pre>
     * This interface accepts portions of a fully specified endpoint, including
     * a region id (see {@link Region}) string if using the Cloud service.
     * <p>
     * A valid endpoint is one of these:
     * <ol>
     * <li>region id string (cloud service only)</li>
     * <li>a string with the syntax [http[s]://]host[:port]</li>
     * </ol>
     * <p>
     * For example, these are valid endpoint arguments:
     * <ul>
     * <li>us-ashburn-1</li>
     * <li>nosql.us-ashburn-1.oci.oraclecloud.com</li>
     * <li>https://nosql.us-ashburn-1.oci.oraclecloud.com:443</li>
     * <li>localhost:8080</li>
     * <li>https://machine-hosting-proxy:443</li>
     * </ul>
     * <p>
     * If using the endpoint (vs region id) syntax, if the port is omitted,
     * the endpoint defaults to 443. If the protocol is omitted, the
     * endpoint uses https if the port is 443, and http in all other cases.
     * <p>
     * If using the Cloud Service see {@link Region} for information on
     * available regions. If using the Cloud Service the constructor,
     * {@link NoSQLHandleConfig#NoSQLHandleConfig(Region,AuthorizationProvider)},
     * is available rather than using a Region's id string.
     *
     * @param endpoint identifies a region id or server for use by the
     * NoSQLHandle. This is a required parameter.
     *
     * @param provider the {@link AuthorizationProvider} to use for the handle
     *
     * @throws IllegalArgumentException if the endpoint is null or malformed.
     */
    public NoSQLHandleConfig(String endpoint, AuthorizationProvider provider) {
        super();
        endpoint = checkRegionId(endpoint, provider);
        this.serviceURL = createURL(endpoint, "/");
        this.authProvider = provider;
        setConfigFromEnvironment();
    }

    /**
     * Cloud service only.
     * <p>
     * Specify a region to use to connect to the Oracle NoSQL Database Cloud
     * Service. The service endpoint will be inferred from the given region.
     * This is the recommended constructor for applications using the Oracle
     * NoSQL Database Cloud Service.
     *
     * @param region identifies the region will be accessed by the NoSQLHandle.
     *
     * @param provider the {@link AuthorizationProvider} to use for the handle
     *
     * @throws IllegalArgumentException if the region is null or malformed.
     */
    public NoSQLHandleConfig(Region region, AuthorizationProvider provider) {
        super();
        requireNonNull(region, "Region must be non-null");
        checkRegion(region, provider);
        this.serviceURL = createURL(region.endpoint(), "/");
        this.region = region;
        this.authProvider = provider;
        setConfigFromEnvironment();
    }

    /**
     * Cloud service only.
     * <p>
     * Specify a {@link AuthorizationProvider} to use to connect to the
     * Oracle NoSQL Database Cloud Service. The service endpoint will be
     * inferred from the given provider. This is the recommended constructor
     * for applications using the Oracle NoSQL Database Cloud Service.
     *
     * @param provider the {@link AuthorizationProvider} to use for the handle
     *
     * @throws IllegalArgumentException if the region is null or malformed.
     */
    public NoSQLHandleConfig(AuthorizationProvider provider) {
        super();
        requireNonNull(provider, "AuthorizationProvider must be non-null");
        if (provider instanceof RegionProvider) {
            this.region = ((RegionProvider) provider).getRegion();
        }
        if (region == null) {
            throw new IllegalArgumentException(
                "Unable to find region from given AuthorizationProvider");
        }
        this.serviceURL = createURL(region.endpoint(), "/");
        this.authProvider = provider;
        setConfigFromEnvironment();
    }

    /**
     * @hidden
     *
     * @param endpoint the endpoint to use
     * @param path the path to use
     * @return the constructed URL
     * Return a URL from an endpoint string
     */
    public static URL createURL(String endpoint, String path) {
        requireNonNull(endpoint,
                       "Endpoint must be non-null");

        /* The defaults for protocol and port */
        String protocol = "https";
        int port = 443;
        String host = null;

        /* Possible formats are:
         * - host
         * - protocol://host
         * - host:port
         * - protocol://host:port
         */

        String[] parts = endpoint.split(":");
        switch (parts.length) {
            case 1:
                /* endpoint is just <host> */
                host = parts[0];
                break;
            case 2:
                /* Could be <protocol>://<host>, or could be <host>:<port> */
                if (parts[0].toLowerCase().startsWith("http")) {
                    protocol = parts[0].toLowerCase();
                    host = parts[1]; /* may have slashes to strip out */
                    if (protocol.equals("http")) {
                        /* Override the default of 443 */
                        port = 8080;
                    }
                } else {
                    host = parts[0];
                    port = validatePort(parts[1], endpoint);
                    if (port != 443) {
                        /* Override the default of https */
                        protocol = "http";
                    }
                }
                break;
             case 3:
                 /* the full <protocol>://<host>:<port> */
                 protocol = parts[0].toLowerCase();
                 host = parts[1];
                 port = validatePort(parts[2], endpoint);
                 break;
             default:
                 throw new IllegalArgumentException("Invalid endpoint: " +
                                                    endpoint);
        }

        /* Strip out any slashes if the format was protocol://host */
        if (host.startsWith("//")) {
            host = host.substring(2);
        }

        try {
            return new URL(protocol, host, port, path);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /*
     * Check that a port is a valid, non negative integer.
     */
    private static int validatePort(String portString, String endpoint) {
        try {
            int port = Integer.parseInt(portString);
            if (port < 0) {
                throw new IllegalArgumentException
                    ("invalid port value of " + port + " for endpoint:" +
                     endpoint);
                }
            return port;
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid port value for " +
                                               "endpoint:" + e);
        }
    }

    /**
     * Sets the URL to use to connect to the service, as alternative to
     * setting the endpont.
     *
     * @param serviceURL a URL to locate a server for use by the NoSQLHandle.
     * It is used and validated in {@link NoSQLHandleFactory#createNoSQLHandle}.
     * This is a required parameter.
     *
     * @throws IllegalArgumentException if the URL is null or malformed.
     */
    public NoSQLHandleConfig(URL serviceURL) {
        super();

        requireNonNull(serviceURL,
                       "NoSQLHandleConfig: serviceURL must be non-null");

        try {
            this.serviceURL = new URL(serviceURL.getProtocol(),
                                      serviceURL.getHost(),
                                      serviceURL.getPort(), "/");
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Returns the version of the driver implemented by this library.
     * @return the version as a string
     */
    static public String getLibraryVersion() {
        return DriverMain.getLibraryVersion();
    }

    /**
     * Returns the URL to use for the {@link NoSQLHandle} connection
     *
     * @return the URL.
     */
    public URL getServiceURL() {
        return serviceURL;
    }

    /**
     * Cloud service only.
     * <p>
     * Returns the region  will be accessed by the NoSQLHandle.
     *
     * @return the region.
     */
    public Region getRegion() {
        return region;
    }

    /**
     * Returns the configured request timeout value, in milliseconds.
     *
     * @return the timeout, in milliseconds, or 0 if it has not been set
     */
    public int getRequestTimeout() {
        return timeout;
    }

    /**
     * Returns the default value for request timeout. If there is no
     * configured timeout or it is configured as 0, a "default" default
     * value of 5000 milliseconds is used.
     *
     * @return the default timeout, in milliseconds
     */
    public int getDefaultRequestTimeout() {
        return timeout == 0 ? DEFAULT_TIMEOUT : timeout;
    }

    /**
     * Returns the configured table request timeout value, in milliseconds.
     * The table request timeout default can be specified independently to allow
     * it to be larger than a typical data request. If it is not specified the
     * default table request timeout of 10000 is used.
     *
     * @return the timeout, in milliseconds, or 0 if it has not been set
     */
    public int getTableRequestTimeout() {
        return tableRequestTimeout;
    }

    /**
     * Returns the default value for a table request timeout. If there is no
     * configured timeout or it is configured as 0, a "default" default
     * value of 10000 milliseconds is used.
     *
     * @return the default timeout, in milliseconds
     */
    public int getDefaultTableRequestTimeout() {
        return tableRequestTimeout == 0 ? DEFAULT_TABLE_REQ_TIMEOUT :
            tableRequestTimeout;
    }

    /**
     * Returns the configured default consistency
     *
     * @return the Consistency, or null if it has not been configured
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Returns the default Consistency value that will be used by the
     * system. If Consistency has been set using {@link #setConsistency}
     * that will be returned. If not a default value of
     * {@link Consistency#EVENTUAL} is returned
     *
     * @return the default Consistency
     */
    public Consistency getDefaultConsistency() {
        return consistency == null ? DEFAULT_CONSISTENCY : consistency;
    }

    /**
     * Returns the {@link RetryHandler} configured for the handle, or null
     * if none is set.
     *
     * @return the handler
     */
    public RetryHandler getRetryHandler() {
        return retryHandler;
    }

    /**
     * Returns the {@link AuthorizationProvider} configured for the handle, or
     * null if none is set.
     *
     * @return the AuthorizationProvider
     */
    public AuthorizationProvider getAuthorizationProvider() {
        return authProvider;
    }

    /**
     * Sets the default request timeout. The default timeout is
     * 5 seconds.
     *
     * @param timeout the timeout value, in milliseconds
     *
     * @return this
     */
    public NoSQLHandleConfig setRequestTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets the default table request timeout.
     * The table request timeout can be specified independently
     * of that specified by {@link #setRequestTimeout} because table requests can
     * take longer and justify longer timeouts. The default timeout is 10
     * seconds (10000 milliseconds).
     *
     * @param tableRequestTimeout the timeout value, in milliseconds
     *
     * @return this
     */
    public NoSQLHandleConfig setTableRequestTimeout(int tableRequestTimeout) {
        this.tableRequestTimeout = tableRequestTimeout;
        return this;
    }

    /**
     * Sets the default request {@link Consistency}. If not set in this
     * object or by a specific request the default consistency used
     * is {@link Consistency#EVENTUAL}
     *
     * @param consistency the Consistency
     *
     * @return this
     */
    public NoSQLHandleConfig setConsistency(Consistency consistency) {
        requireNonNull(
            consistency,
            "NoSQLHandleConfig.setConsistency: consistency must be non-null");

        this.consistency = consistency;
        return this;
    }

    /**
     * Sets the number of threads to use for handling
     * network traffic. This number affects the performance of concurrent
     * requests in a multithreaded application. If set to 0 or not
     * modified the default is the number of CPUs available * 2
     *
     * @param numThreads the number
     *
     * @return this
     */
    public NoSQLHandleConfig setNumThreads(int numThreads) {
        if (numThreads < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setNumThreads: numThreads must " +
                "be a non-negative value");
        }
        this.numThreads = numThreads;
        return this;
    }

    /**
     * Sets the maximum number of individual connections to use to connect
     * to the service. Each request/response pair uses a connection. The
     * pool exists to allow concurrent requests and will bound the number of
     * concurrent requests. Additional requests will wait for a connection to
     * become available. If requests need to wait for a significant time
     * additional connections may be created regardless of the pool size.
     * The default value if not set is number of available CPUs * 2.
     *
     * @param poolSize the pool size
     *
     * @return this
     * @deprecated The connection pool no longer supports a size setting.
     * It will expand as needed based on concurrent demand.
     */
    @Deprecated
    public NoSQLHandleConfig setConnectionPoolSize(int poolSize) {
        return this;
    }

    /**
     * Sets the minimum number of connections to keep in the connection pool
     * when the connections are inactive. This number is used to generate
     * keep-alive messages that prevent this many connections from timing out in
     * environments that can time out, such as the NoSQL Cloud Service.
     * This setting can reduce the latency required to re-create secure
     * connections after an application goes idle for a while (minutes).
     * <p>
     * If this value is 0 (default) the minimum is set to 2.
     * If set to -1 then all connections are allowed to time out.
     * If the number of connections in the pool never reaches this minimum,
     * but the minimum is set, those connections will be kept alive. Additional
     * connections are only created on demand. This setting can be thought of
     * as a low-water mark.
     *
     * @param poolMinSize the minimum pool size
     *
     * @return this
     *
     * @since 5.3.2
     */
    public NoSQLHandleConfig setConnectionPoolMinSize(int poolMinSize) {
        this.connectionPoolMinSize = poolMinSize;
        return this;
    }

    /**
     * @hidden
     *
     * Sets an inactivity period, in seconds, used to time out idle connections
     * in the connection pool. This setting allows unused connections to be
     * reclaimed when the system goes idle, reducing resource use. If 0 for
     * the default value. A negative number means inactive connections are not
     * timed out.
     *
     * @param poolInactivityPeriod the period, in seconds
     *
     * @return this
     *
     * @since 5.3.2
     */
    public NoSQLHandleConfig setConnectionPoolInactivityPeriod(
        int poolInactivityPeriod) {
        this.connectionPoolInactivityPeriod = poolInactivityPeriod;
        return this;
    }

    /**
     * Sets the maximum number of pending acquire operations allowed on the
     * connection pool. This number is used if the degree of concurrency
     * desired exceeds the size of the connection pool temporarily. The
     * default value is 3.
     *
     * @param poolMaxPending the maximum number allowed
     *
     * @return this
     * @deprecated The connection pool no longer supports pending requests.
     */
    @Deprecated
    public NoSQLHandleConfig setPoolMaxPending(int poolMaxPending) {
        return this;
    }

    /**
     * Sets the maximum size in bytes of request/response payloads.
     * On-premise only. This setting is ignored for cloud operations.
     * If not set, or set to zero, the default value of 32MB is used.
     *
     * @param maxContentLength the maximum bytes allowed in
     * requests/responses. Pass zero to use the default.
     *
     * @return this
     */
    public NoSQLHandleConfig setMaxContentLength(int maxContentLength) {
        if (maxContentLength < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setMaxContentLength: maxContentLength must " +
                "not be negative");
        }
        this.maxContentLength = maxContentLength;
        return this;
    }

    /**
     * Returns the maximum size, in bytes, of a request operation payload.
     * On-premise only. This value is ignored for cloud operations.
     *
     * @return the size
     */
    public int getMaxContentLength() {
        return maxContentLength;
    }

    /**
     * @hidden
     * Sets the maximum size in bytes of http chunks.
     * If not set, or set to zero, the default value of 64KB is used.
     *
     * @param maxChunkSize the maximum bytes allowed in
     * http chunks. Pass zero to use the default.
     *
     * @return this
     */
    public NoSQLHandleConfig setMaxChunkSize(int maxChunkSize) {
        if (maxChunkSize < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setMaxChunkSize: maxChunkSize must " +
                "not be negative");
        }
        this.maxChunkSize = maxChunkSize;
        return this;
    }

    /**
     * @hidden
     * Returns the maximum size, in bytes, of any http chunk
     *
     * @return the size
     */
    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    /**
     * Returns the maximum number of individual connections to use to connect
     * to the service. Each request/response pair uses a connection. The
     * pool exists to allow concurrent requests and will bound the number of
     * concurrent requests. Additional requests will wait for a connection to
     * become available.
     *
     * @return 0
     * @deprecated The connection pool no longer supports a size setting.
     * It will expand as needed based on concurrent demand.
     */
    @Deprecated
    public int getConnectionPoolSize() {
        return 0;
    }

    /**
     * Returns the minimum number of connections to keep alive in the connection
     * pool.
     *
     * @return the minimum pool size or 0 if not set
     *
     * @since 5.3.2
     */
    public int getConnectionPoolMinSize() {
        return connectionPoolMinSize;
    }

    /**
     * @hidden
     * Returns the inactivity period, in seconds, to use to time out idle
     * connections. This allows the connection pool to shrink after a period
     * of inactivity, reducing resource use.
     *
     * @return the inactivity period, or 0 if not set
     *
     * @since 5.3.2
     */
    public int getConnectionPoolInactivityPeriod() {
        return connectionPoolInactivityPeriod;
    }

    /**
     * Returns the maximum number of pending acquire operations allowed on
     * the connection pool.
     *
     * @return 0
     * @deprecated The connection pool no longer supports pending requests.
     */
    @Deprecated
    public int getPoolMaxPending() {
        return 0;
    }

    /**
     * Returns the number of threads to use for handling
     * network traffic.
     *
     * @return the number of threads or 0 if not set
     */
    public int getNumThreads() {
        return numThreads;
    }

    /**
     * Sets the {@link RetryHandler} to use for the handle. If no handler is
     * configured a default is used. The handler must be safely usable by
     * multiple threads.
     *
     * @param retryHandler the handler
     *
     * @return this
     */
    public NoSQLHandleConfig setRetryHandler(RetryHandler retryHandler) {
        requireNonNull(
            retryHandler,
            "NoSQLHandleConfig.setRetryHandler: retryHandler must be non-null");

        this.retryHandler = retryHandler;
        return this;
    }

    /**
     * Sets the {@link AuthorizationProvider} to use for the handle. The
     * provider must be safely usable by multiple threads.
     *
     * @param provider the AuthorizationProvider
     *
     * @return this
     */
    public NoSQLHandleConfig
        setAuthorizationProvider(AuthorizationProvider provider) {

        requireNonNull(provider, "NoSQLHandleConfig.setAuthorizationProvider" +
            ": provider must be non-null");

        this.authProvider = provider;
        return this;
    }

    /**
     * Enables internal rate limiting.
     * Cloud service only.
     * @param enable If true, enable internal rate limiting.
     *               If false, disable internal rate limiting.
     * @return this
     */
    public NoSQLHandleConfig setRateLimitingEnabled(boolean enable) {
        this.rateLimitingEnabled = enable;
        return this;
    }

    /**
     * @hidden
     * Internal use only
     * @return true if rate limiting is enabled
     */
    public boolean getRateLimitingEnabled() {
        return rateLimitingEnabled;
    }

    /**
     * Sets a default rate limiter use percentage.
     * Cloud service only.
     * <p>
     * Sets a default percentage of table limits to use. This may be
     * useful for cases where a client should only use a portion of
     * full table limits. This only applies if rate limiting is enabled using
     * {@link #setRateLimitingEnabled}.
     * <p>
     * The default for this value is 100.0 (full table limits).
     *
     * @param percent the percentage of table limits to use. This value
     *        must be positive.
     */
    public void setDefaultRateLimitingPercentage(double percent) {
        if (percent <= 0.0) {
            throw new IllegalArgumentException(
                "rate limiter percentage must be positive");
        }
        defaultRateLimiterPercentage = percent;
    }

    /**
     * @hidden
     * Internal use only
     * @return the default percentage
     */
    public double getDefaultRateLimitingPercentage() {
        if (defaultRateLimiterPercentage == 0.0) {
            return 100.0;
        }
        return defaultRateLimiterPercentage;
    }

    /**
     * Sets the {@link RetryHandler} using a default retry handler configured
     * with the specified number of retries and a static delay.
     * A delay of 0 means "use the default delay algorithm" which is an
     * incremental backoff algorithm. A non-zero delay will work but is not
     * recommended for production systems as it is not flexible.
     *<p>
     * The default retry handler will not retry exceptions of type
     * {@link OperationThrottlingException}. The reason is that these
     * operations are long-running, and while technically they can be
     * retried, an immediate retry is unlikely to succeed because of the
     * low rates allowed for these operations.
     *
     * @param numRetries the number of retries to perform automatically.
     * This parameter may be 0 for no retries.
     * @param delayMS the delay, in milliseconds. Pass 0 to use the default
     * delay algorithm.
     *
     * @return this
     */
    public NoSQLHandleConfig configureDefaultRetryHandler(int numRetries,
                                                          int delayMS) {
        retryHandler = new DefaultRetryHandler(numRetries, delayMS);
        return this;
    }

    /**
     * Sets the Logger used for the driver.
     *
     * @param logger the Logger.
     *
     * @return this
     */
    public NoSQLHandleConfig setLogger(Logger logger) {
        requireNonNull(logger,
                       "NoSQLHandleConfig.setLogger: logger must be non-null");

        this.logger = logger;
        return this;
    }

    /**
     * Returns the Logger, or null if not configured by user.
     *
     * @return the Logger
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Cloud service only.
     *
     * Sets the default compartment to use for requests sent using the
     * handle. Setting the default is optional and if set it is overridden
     * by any compartment specified in a request or table name. If no
     * compartment is set for a request, either using this default or
     * by specification in a request, the behavior varies with how the
     * application is authenticated.
     * <ul>
     * <li>If authenticated with a user identity the default is the root
     * compartment of the tenancy</li>
     * <li>If authenticated as an instance principal (see
     * {@link SignatureProvider#createWithInstancePrincipal}) the
     * compartment id (OCID )must be specified by either using this method or
     * in each Request object. If not an exception is thrown</li>
     * </ul>
     *
     * @param compartment may be either the name of a compartment or the
     * id (OCID) of a compartment.
     *
     * @return this
     */
    public NoSQLHandleConfig setDefaultCompartment(String compartment) {
        this.compartment = compartment;
        return this;
    }

    /**
     * Cloud service only.
     *
     * Returns the default compartment to use for requests or null if not set.
     * The value may be a compartment name or id, as set by
     * {@link #setDefaultCompartment}.
     *
     * @return the compartment
     */
    public String getDefaultCompartment() {
        return compartment;
    }

    /**
     * Returns the SSL cipher suites to enable.
     * @return cipher suites.
     */
    public List<String> getSSLCipherSuites() {
        return ciphers;
    }

    /**
     * Returns the SSL protocols to enable.
     * @return cipher suites.
     */
    public String[] getSSLProtocols() {
        if (protocols == null) {
            return null;
        }
        return protocols.stream().toArray(String[] :: new);
    }

    /**
     * Returns the configured SSLSession objects timeout, in seconds.
     *
     * @return the timeout, in seconds, or 0 if it has not been set
     */
    public int getSSLSessionTimeout() {
        return sslSessionTimeout;
    }

    /**
     * Returns the configured size of cache used to store SSLSession objects.
     *
     * @return the cache size
     */
    public int getSSLSessionCacheSize() {
        return sslSessionCacheSize;
    }

    /**
     * Returns the configured SSL handshake timeout, in milliseconds.
     *
     * @return the timeout, in milliseconds, or 0 if it has not been set
     * @since 5.3.2
     */
    public int getSSLHandshakeTimeout() {
        return sslHandshakeTimeoutMs;
    }

    /**
     * Set SSL cipher suites to enable, in the order of preference. null to
     * use default cipher suites.
     *
     * @param cipherSuites cipher suites list
     * @return this
     */
    public NoSQLHandleConfig setSSLCipherSuites(String... cipherSuites) {
        if (ciphers == null) {
            ciphers = new ArrayList<>(Arrays.asList(cipherSuites));
        } else {
            ciphers.addAll(Arrays.asList(cipherSuites));
        }
        return this;
    }

    /**
     * Set SSL protocols to enable, in the order of preference. null to
     * use default protocols.
     *
     * @param sslProtocols SSL protocols list
     * @return this
     */
    public NoSQLHandleConfig setSSLProtocols(String... sslProtocols) {
        if (protocols == null) {
            protocols = new ArrayList<>(Arrays.asList(sslProtocols));
        } else {
            protocols.addAll(Arrays.asList(sslProtocols));
        }
        for (String protocol : protocols) {
            if (!VALID_SSL_PROTOCOLS.contains(protocol)) {
                throw new IllegalArgumentException(
                    "NoSQLHandleConfig.setSSLProtocols: " + protocol +
                    " is not a valid SSL protocol name. Must be one of " +
                    VALID_SSL_PROTOCOLS);
            }
        }
        return this;
    }

    /**
     * Sets the size of the cache used for storing SSL session objects, 0
     * to use the default value, no size limit.
     *
     * @param cacheSize the size of SSLSession objects.
     *
     * @return this
     */
    public NoSQLHandleConfig setSSLSessionCacheSize(int cacheSize) {
        if (cacheSize < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setSSLSessionCacheSize: cacheSize must " +
                "be a positive value or zero");
        }
        this.sslSessionCacheSize = cacheSize;
        return this;
    }

    /**
     * Sets the timeout for the cached SSL session objects, in seconds. 0 to
     * use the default value, no limit. When the timeout limit is exceeded for
     * a session, the SSLSession object is invalidated and future connections
     * cannot resume or rejoin the session.
     *
     * @param timeout the session timeout
     *
     * @return this
     */
    public NoSQLHandleConfig setSSLSessionTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setSSLSessionTimeout: timeout must " +
                "be a positive value or zero");
        }
        this.sslSessionTimeout = timeout;
        return this;
    }

    /**
     * Sets the timeout for the SSL handshake, in milliseconds. 0 to use the
     * default value, 3000 milliseconds. In general the default works. This
     * value can be set to help debug suspected SSL issues and force
     * retries within the request timeout period.
     *
     * @param timeout the SSL handshake timeout
     *
     * @return this
     * @since 5.3.2
     */
    public NoSQLHandleConfig setSSLHandshakeTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException(
                "NoSQLHandleConfig.setSSLHandshakeTimeout: timeout must " +
                "be a positive value or zero");
        }
        this.sslHandshakeTimeoutMs = timeout;
        return this;
    }

    /**
     * Sets an HTTP proxy host to be used for the session. If a proxy host
     * is specified a proxy port must also be specified, using
     * {@link #setProxyPort}.
     *
     * @param proxyHost the proxy host
     *
     * @return this
     */
    public NoSQLHandleConfig setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
        return this;
    }

    /**
     * Sets an HTTP proxy user name if the configured proxy host requires
     * authentication. If a proxy host is not configured this configuration
     * is ignored. If a proxy user name is configure a proxy password must
     * also be configured, using {@link #setProxyPassword}.
     *
     * @param proxyUsername the user name
     *
     * @return this
     */
    public NoSQLHandleConfig setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
        return this;
    }

    /**
     * Sets an HTTP proxy password if the configured proxy host requires
     * authentication. If a proxy password is configured a proxy user name
     * must also be configured using {@link #setProxyUsername}.
     *
     * @param proxyPassword the password
     *
     * @return this
     */
    public NoSQLHandleConfig setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
        return this;
    }

    /**
     * Sets an HTTP proxy port to be used for the session. If a proxy port
     * is specified a proxy host must also be specified, using
     * {@link #setProxyHost}.
     *
     * @param proxyPort the proxy port
     *
     * @return this
     */
    public NoSQLHandleConfig setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
        return this;
    }

    /**
     * @hidden
     * Returns a proxy host, or null if not configured
     *
     * @return the host, or null
     */
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * @hidden
     * Returns a proxy user name, or null if not configured
     *
     * @return the user name, or null
     */
    public String getProxyUsername() {
        return proxyUsername;
    }

    /**
     * @hidden
     * Returns a proxy password, or null if not configured
     *
     * @return the password, or null
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    /**
     * @hidden
     * Returns a proxy port, or 0 if not configured
     *
     * @return the proxy port
     */
    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * @hidden
     * @param sslCtx the SSL context to set
     * @return this
     */
    public NoSQLHandleConfig setSslContext(SslContext sslCtx) {
        this.sslCtx = sslCtx;
        return this;
    }

    /**
     * @hidden
     * @return the SSL context
     */
    public SslContext getSslContext() {
        return sslCtx;
    }

    /**
     * Sets interval size in seconds for logging statistics.
     * Default interval is 600 seconds, i.e. 10 min.
     *
     * @param statsInterval stats logging interval in seconds
     * @return this
     *
     * @since 5.2.30
     */
    public NoSQLHandleConfig setStatsInterval(int statsInterval) {
        if (statsInterval < 1) {
            throw new IllegalArgumentException("Stats interval can not be " +
                "less than 1 second.");
        }
        this.statsInterval = statsInterval;
        return this;
    }

    /**
     * Returns the current interval for logging statistics.
     * Default interval is 600 seconds, i.e. 10 min.
     *
     * @return the current interval in seconds
     *
     * @since 5.2.30
     */
    public int getStatsInterval() {
        return this.statsInterval;
    }

    /**
     * Set the statistics collection profile.
     * Default profile is NONE.
     *
     * @param statsProfile profile to use
     * @return this
     *
     * @since 5.2.30
     */
    public NoSQLHandleConfig setStatsProfile(StatsControl.Profile statsProfile)
    {
        this.statsProfile = statsProfile;
        return this;
    }

    /**
     * Returns the statistics collection profile.
     * Default profile is NONE.
     *
     * @return the current profile
     *
     * @since 5.2.30
     */
    public StatsControl.Profile getStatsProfile() {
        return this.statsProfile;
    }

    /**
     * Enable JSON pretty print for easier human reading when logging
     * statistics.
     * Default is disabled.
     *
     * @param statsPrettyPrint flag to enable JSON pretty print
     * @return this
     *
     * @since 5.2.30
     */
    public NoSQLHandleConfig setStatsPrettyPrint(boolean statsPrettyPrint) {
        this.statsPrettyPrint = statsPrettyPrint;
        return this;
    }

    /**
     * Returns the current JSON pretty print flag for logging statistics.
     * Default is disabled.
     *
     * @return the current JSON pretty print flag
     *
     * @since 5.2.30
     */
    public boolean getStatsPrettyPrint() {
        return this.statsPrettyPrint;
    }

    /**
     * When stats are enabled the logging is automatically turned on. Setting
     * this to false avoids turning the log on when enabling stats.
     * Default is on.
     *
     * @param statsEnableLog flag to enable JSON pretty print
     * @return this
     *
     * @since 5.2.30
     */
    public NoSQLHandleConfig setStatsEnableLog(boolean statsEnableLog) {
        this.statsEnableLog = statsEnableLog;
        return this;
    }

    /**
     * Returns the current value of enabling the log when stats are turned on.
     * Default is enabled.
     *
     * @return the current state of stats enable log flag.
     *
     * @since 5.2.30
     */
    public boolean getStatsEnableLog() {
        return this.statsEnableLog;
    }

    /**
     * Registers a handler that is called every time the statistics are logged.
     * Note: setting a stats handler will not affect the stats log entries.
     * @param statsHandler User defined StatsHandler.
     *
     * @return this
     *
     * @since 5.2.30
     */
    public NoSQLHandleConfig setStatsHandler(
        StatsControl.StatsHandler statsHandler) {
        this.statsHandler = statsHandler;
        return this;
    }

    /**
     * Returns the registered statistics handler, otherwise null.
     *
     * @return this
     *
     * @since 5.2.30
     */
    public StatsControl.StatsHandler getStatsHandler() {
        return statsHandler;
    }

    /**
     * @hidden
     *
     * Cloud service only.
     * Turns on, or off internal, automatic refresh of auth information based on
     * tracked requests. This is present in case the refresh really isn't
     * desired or the mechanism fails for some reason. This mechanism is off
     * by default.
     *
     * @param value true to enable refresh
     * @return this
     *
     * @since 5.3.2
     */
    public NoSQLHandleConfig setAuthRefresh(boolean value) {
        this.authRefresh = value;
        return this;
    }

    /**
     * @hidden
     *
     * Cloud service only.
     * Returns the state of the authRefresh flag
     *
     * @return true if auth refresh is enabled
     *
     * @since 5.3.2
     */
    public boolean getAuthRefresh() {
        return authRefresh;
    }

    @Override
    public NoSQLHandleConfig clone() {
        try {
            NoSQLHandleConfig clone = (NoSQLHandleConfig) super.clone();
            return clone;
        } catch (CloneNotSupportedException neverHappens) {
            return null;
        }
    }

    static private String checkRegionId(String endpoint,
                                        AuthorizationProvider provider) {
        try {
            Region region = Region.fromRegionId(endpoint);
            if (region != null) {
                checkRegion(region, provider);
                return region.endpoint();
            }
        } catch (IllegalArgumentException iae) {
            /* ignore */
        }
        return endpoint;
    }

    static private void checkRegion(Region region,
                                    AuthorizationProvider provider) {
        if (provider instanceof RegionProvider) {
            Region regionInProvider = ((RegionProvider) provider).getRegion();
            if (regionInProvider != null && regionInProvider != region) {
                throw new IllegalArgumentException(
                   "Specified region " + region + " doesn't match the region " +
                   " in AuthorizationProvider");
            }
        }
    }

    private void setConfigFromEnvironment() {
        String profileProp = System.getProperty(STATS_PROFILE_PROPERTY);
        if (profileProp != null) {
            try {
                setStatsProfile(StatsControl.Profile.valueOf(
                    profileProp.toUpperCase()));
            } catch (IllegalArgumentException iae) {
                if (logger != null) {
                    logger.log(Level.SEVERE, StatsControl.LOG_PREFIX +
                        "Invalid profile value for system property " +
                        STATS_PROFILE_PROPERTY + ": " + profileProp);
                }
            }
        }

        String intervalProp = System.getProperty(STATS_INTERVAL_PROPERTY);
        if (intervalProp != null) {
            try {
                setStatsInterval(Integer.valueOf(intervalProp));
            } catch (NumberFormatException nfe) {
                if (logger != null) {
                    logger.log(Level.SEVERE, "Invalid integer value for " +
                        "system property " + STATS_INTERVAL_PROPERTY + ": " +
                        intervalProp);
                }
            }
        }

        String ppProp = System.getProperty(STATS_PRETTY_PRINT_PROPERTY);
        if (ppProp != null &&
            ("true".equals(ppProp.toLowerCase()) || "1".equals(ppProp) ||
             "on".equals(ppProp.toLowerCase()))) {
            statsPrettyPrint = Boolean.valueOf(ppProp);
        }

        String elProp = System.getProperty(STATS_ENABLE_LOG_PROPERTY);
        if (elProp != null &&
            ("false".equals(elProp.toLowerCase()) || "0".equals(elProp) ||
                "off".equals(elProp.toLowerCase()))) {
            statsEnableLog = Boolean.FALSE;
        }
    }

    /**
     * Returns the set extension to the user agent http header or null if
     * unset.
     */
    public String getExtensionUserAgent() {
        return extensionUserAgent;
    }

    /**
     * Sets an extension to the user agent http header. Extension must be
     * up to 64 chars long.
     */
    public void setExtensionUserAgent(String extensionUserAgent) {
        if (extensionUserAgent != null && extensionUserAgent.length() > 64) {
            throw new IllegalArgumentException("User agent extension too " +
                "long, must be up to 64 chars long: " +
                extensionUserAgent.length());
        }
        this.extensionUserAgent = extensionUserAgent;
    }
}
