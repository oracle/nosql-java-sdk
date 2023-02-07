/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.driver.ops.TableLimits.CapacityMode;
import static oracle.nosql.driver.util.BinaryProtocol.DEFAULT_SERIAL_VERSION;
import static oracle.nosql.driver.util.BinaryProtocol.TABLE_NOT_FOUND;
import static oracle.nosql.driver.util.BinaryProtocol.V2;
import static oracle.nosql.driver.util.BinaryProtocol.V3;
import static oracle.nosql.driver.util.BinaryProtocol.V4;
import static oracle.nosql.driver.util.CheckNull.requireNonNull;
import static oracle.nosql.driver.util.HttpConstants.ACCEPT;
import static oracle.nosql.driver.util.HttpConstants.CONNECTION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_LENGTH;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.driver.util.HttpConstants.COOKIE;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_NAMESPACE_HEADER;
import static oracle.nosql.driver.util.HttpConstants.NOSQL_DATA_PATH;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.driver.util.HttpConstants.USER_AGENT;
import static oracle.nosql.driver.util.HttpConstants.X_RATELIMIT_DELAY;
import static oracle.nosql.driver.util.LogUtil.isLoggable;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.LogUtil.logTrace;
import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.DefaultRetryHandler;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.OperationNotSupportedException;
import oracle.nosql.driver.RateLimiter;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.RequestSizeLimitException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.RetryHandler;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DurableRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.HttpConstants;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.RateLimiterMap;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.MapValue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;

/**
 * The HTTP driver client.
 */
public class Client {

    public static int traceLevel = 0;

    private final NoSQLHandleConfig config;

    private final SerializerFactory v3factory = new BinarySerializerFactory();
    private final SerializerFactory v4factory = new NsonSerializerFactory();

    /**
     * The URL representing the server that is the target of all client
     * requests.
     */
    private final URL url;

    /**
     * The fixed constant URI path associated with all KV requests.
     */
    private final String kvRequestURI;

    /**
     * The host/port components of the URL, decomposed here for efficient access.
     */
    private final String host;

    /**
     * Tracks the unique client scoped request id.
     */
    private final AtomicInteger maxRequestId = new AtomicInteger(1);

    private final HttpClient httpClient;

    private final AuthorizationProvider authProvider;

    private final boolean useSSL;

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final Logger logger;

    /*
     * Internal rate limiting: cloud only
     */
    private RateLimiterMap rateLimiterMap;

    /*
     * Keep an internal map of tablename to last limits update time
     */
    private Map<String, AtomicLong> tableLimitUpdateMap;

    /* update table limits once every 10 minutes */
    private static long LIMITER_REFRESH_NANOS = 600_000_000_000L;

    /*
     * amount of time between retries when security information
     * is unavailable
     */
    private static final int SEC_ERROR_DELAY_MS = 100;

    /*
     * singe thread executor for updating table limits
     */
    private ExecutorService threadPool;

    private volatile short serialVersion = DEFAULT_SERIAL_VERSION;

    /* for one-time messages */
    private final HashSet<String> oneTimeMessages;

    /**
     * config for statistics
     */
    private StatsControlImpl statsControl;

    /**
     * list of Request instances to refresh when auth changes. This will only
     * exist in a cloud configuration
     */
    private ConcurrentLinkedQueue<Request> authRefreshRequests;
    /* used as key and value for auth requests -- guaranteed illegal */
    private MapValue badValue;

    /*
     * for session persistence, if used. This has the
     * full "session=xxxxx" key/value pair.
     */
    private volatile String sessionCookie;
    /* note this must end with '=' */
    private final String SESSION_COOKIE_FIELD = "session=";

    /* for keeping track of SDKs usage */
    private String userAgent;

    public Client(Logger logger,
                  NoSQLHandleConfig httpConfig) {

        this.logger = logger;
        this.config = httpConfig;
        this.url = httpConfig.getServiceURL();

        logFine(logger, "Driver service URL:" + url.toString());
        final String protocol = httpConfig.getServiceURL().getProtocol();
        if (!("http".equalsIgnoreCase(protocol) ||
              "https".equalsIgnoreCase(protocol))) {
            throw new IllegalArgumentException("Unknown protocol:" + protocol);
        }

        kvRequestURI = httpConfig.getServiceURL().toString() + NOSQL_DATA_PATH;
        host = httpConfig.getServiceURL().getHost();

        useSSL = "https".equalsIgnoreCase(protocol);

        /*
         * This builds an insecure context, usable for testing only
         */
        SslContext sslCtx = null;
        if (useSSL) {
            sslCtx = config.getSslContext();
            if (sslCtx == null) {
                throw new IllegalArgumentException(
                    "Unable to configure https: " +
                    "SslContext is missing from config");
            }
        }

        /*
         * create the HttpClient instance.
         */
        httpClient = new HttpClient(
            url.getHost(),
            url.getPort(),
            httpConfig.getNumThreads(),
            httpConfig.getConnectionPoolMinSize(),
            httpConfig.getConnectionPoolInactivityPeriod(),
            httpConfig.getMaxContentLength(),
            httpConfig.getMaxChunkSize(),
            sslCtx,
            config.getSSLHandshakeTimeout(),
            "NoSQL Driver",
            logger);
        if (httpConfig.getProxyHost() != null) {
            httpClient.configureProxy(httpConfig);
        }

        authProvider= config.getAuthorizationProvider();
        if (authProvider == null) {
            throw new IllegalArgumentException(
                "Must configure AuthorizationProvider to use HttpClient");
        }

        /* StoreAccessTokenProvider == onprem */
        if (config.getRateLimitingEnabled() &&
            !(authProvider instanceof StoreAccessTokenProvider)) {
            logFine(logger, "Starting client with rate limiting enabled");
            rateLimiterMap = new RateLimiterMap();
            tableLimitUpdateMap = new ConcurrentHashMap<String, AtomicLong>();
            threadPool = Executors.newSingleThreadExecutor();
        } else {
            logFine(logger, "Starting client with no rate limiting");
            rateLimiterMap = null;
            tableLimitUpdateMap = null;
            threadPool = null;
        }

        oneTimeMessages = new HashSet<String>();
        statsControl = new StatsControlImpl(config,
            logger, httpClient, httpConfig.getRateLimitingEnabled());

        String extensionUserAgent = httpConfig.getExtensionUserAgent();
        if (extensionUserAgent != null) {
            userAgent = new StringBuilder(HttpConstants.userAgent)
                .append(" ")
                .append(extensionUserAgent)
                .toString();
        } else {
            this.userAgent = HttpConstants.userAgent;
        }
    }

    /**
     * Shutdown the client
     *
     * TODO: add optional timeout (needs change in HttpClient)
     */
    public void shutdown() {
        logFine(logger, "Shutting down driver http client");
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        httpClient.shutdown();
        statsControl.shutdown();
        if (authProvider != null) {
            authProvider.close();
        }
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public int getAcquiredChannelCount() {
        return httpClient.getAcquiredChannelCount();
    }

    public int getTotalChannelCount() {
        return httpClient.getTotalChannelCount();
    }

    public int getFreeChannelCount() {
        return httpClient.getFreeChannelCount();
    }

    /**
     * Get the next client-scoped request id. It needs to be combined with the
     * client id to obtain a globally unique scope.
     */
    private int nextRequestId() {
        return maxRequestId.addAndGet(1);
    }

    /**
     * Execute the KV request and return the response. This is the top-level
     * method for request execution.
     * <p>
     * This method handles exceptions to distinguish between what can be retried
     * and what cannot, making sure that root cause exceptions are
     * kept. Examples:
     * <ul>
     *  <li>can't connect (host, port, etc)</li>
     *  <li>throttling exceptions</li>
     *  <li>general networking issues, IOException</li>
     * </ul>
     *
     * RequestTimeoutException needs a cause, or at least needs to include the
     * message from the causing exception.
     *
     * @param kvRequest the KV request to be executed by the server
     *
     * @return the Result of the request
     */
    public Result execute(Request kvRequest) {

        requireNonNull(kvRequest, "NoSQLHandle: request must be non-null");

        /*
         * Before execution, call Request object to assign default values
         * from config object if they are not overridden. This allows code
         * to assume that all potentially defaulted parameters (timeouts, etc)
         * are explicit when they are sent on the wire.
         */
        kvRequest.setDefaults(config);

        /*
         * Validate the request, checking for required state, etc. If this
         * fails for a given Request instance it will throw
         * IllegalArgumentException.
         */
        kvRequest.validate();

        /* clear any retry stats that may exist on this request object */
        kvRequest.setRetryStats(null);
        kvRequest.setRateLimitDelayedMs(0);

        if (kvRequest.isQueryRequest()) {
            QueryRequest qreq = (QueryRequest)kvRequest;

            statsControl.observeQuery(qreq);

            /*
             * The following "if" may be true for advanced queries only. For
             * such queries, the "if" will be true (i.e., the QueryRequest will
             * be bound with a QueryDriver) if and only if this is not the 1st
             * execute() call for this query. In this case we just return a new,
             * empty QueryResult. Actual computation of a result batch will take
             * place when the app calls getResults() on the QueryResult.
             */
            if (qreq.hasDriver()) {
                trace("QueryRequest has QueryDriver", 2);
                return new QueryResult(qreq, false);
            }

            /*
             * If it is an advanced query and we are here, then this must be
             * the 1st execute() call for the query. If the query has been
             * prepared before, we create a QueryDriver and bind it with the
             * QueryRequest. Then, we create and return an empty QueryResult.
             * Actual computation of a result batch will take place when the
             * app calls getResults() on the QueryResult.
             */
            if (qreq.isPrepared() && !qreq.isSimpleQuery()) {
                trace("QueryRequest has no QueryDriver, but is prepared", 2);
                QueryDriver driver = new QueryDriver(qreq);
                driver.setClient(this);
                driver.setTopologyInfo(qreq.topologyInfo());
                return new QueryResult(qreq, false);
            }

            /*
             * If we are here, then this is either (a) a simple query or (b) an
             * advanced query that has not been prepared already, which also
             * implies that this is the 1st execute() call on this query. For
             * a non-prepared advanced query, the effect of this 1st execute()
             * call is to send the query to the proxy for compilation, get back
             * the prepared query, but no query results, create a QueryDriver,
             * and bind it with the QueryRequest (see
             * QueryRequestSerializer.deserialize()), and return an empty
             * QueryResult.
             */
            trace("QueryRequest has no QueryDriver and is not prepared", 2);
        }

        int timeoutMs = kvRequest.getTimeoutInternal();

        Throwable exception = null;

        /*
         * If the request doesn't set an explicit compartment, use
         * the config default if provided.
         */
        if (kvRequest.getCompartment() == null) {
            kvRequest.setCompartmentInternal(
                config.getDefaultCompartment());
        }

        int rateDelayedMs = 0;
        boolean checkReadUnits = false;
        boolean checkWriteUnits = false;

        /* if the request itself specifies rate limiters, use them */
        RateLimiter readLimiter = kvRequest.getReadRateLimiter();
        if (readLimiter != null) {
            checkReadUnits = true;
        }
        RateLimiter writeLimiter = kvRequest.getWriteRateLimiter();
        if (writeLimiter != null) {
            checkWriteUnits = true;
        }

        /* if not, see if we have limiters in our map for the given table */
        if (rateLimiterMap != null &&
            readLimiter == null && writeLimiter == null) {
            String tableName = kvRequest.getTableName();
            if (tableName != null && tableName.length() > 0) {
                readLimiter = rateLimiterMap.getReadLimiter(tableName);
                writeLimiter = rateLimiterMap.getWriteLimiter(tableName);
                if (readLimiter == null && writeLimiter == null) {
                    if (kvRequest.doesReads() || kvRequest.doesWrites()) {
                        backgroundUpdateLimiters(tableName,
                                                 kvRequest.getCompartment());
                    }
                } else {
                    checkReadUnits = kvRequest.doesReads();
                    kvRequest.setReadRateLimiter(readLimiter);
                    checkWriteUnits = kvRequest.doesWrites();
                    kvRequest.setWriteRateLimiter(writeLimiter);
                }
            }
        }

        final long startTime = System.currentTimeMillis();
        kvRequest.setStartTimeMs(startTime);
        final String requestClass = kvRequest.getClass().getSimpleName();

        String requestId = "";
        int thisIterationTimeoutMs = 0;

        do {
            long thisTime = System.currentTimeMillis();
            thisIterationTimeoutMs = timeoutMs - (int)(thisTime - startTime);

            /*
             * Check rate limiters before executing the request.
             * Wait for read and/or write limiters to be below their limits
             * before continuing. Be aware of the timeout given.
             */
            if (readLimiter != null && checkReadUnits == true) {
                try {
                    /*
                     * this may sleep for a while, up to thisIterationTimeoutMs
                     * and may throw TimeoutException
                     */
                    rateDelayedMs += readLimiter.consumeUnitsWithTimeout(
                        0, thisIterationTimeoutMs, false);
                } catch (Exception e) {
                    exception = e;
                    break;
                }
            }
            if (writeLimiter != null && checkWriteUnits == true) {
                try {
                    /*
                     * this may sleep for a while, up to thisIterationTimeoutMs
                     * and may throw TimeoutException
                     */
                    rateDelayedMs += writeLimiter.consumeUnitsWithTimeout(
                        0, thisIterationTimeoutMs, false);
                } catch (Exception e) {
                    exception = e;
                    break;
                }
            }

            /* ensure limiting didn't throw us over the timeout */
            if (timeoutRequest(startTime, timeoutMs, exception)) {
                break;
            }

            /* update iteration timeout in case limiters slept for some time */
            thisTime = System.currentTimeMillis();
            thisIterationTimeoutMs = timeoutMs - (int)(thisTime - startTime);

            final String authString =
                authProvider.getAuthorizationString(kvRequest);
            authProvider.validateAuthString(authString);

            if (kvRequest.getNumRetries() > 0) {
                logRetries(kvRequest.getNumRetries(), exception);
            }

            if (serialVersion < 3 && kvRequest instanceof DurableRequest) {
                if (((DurableRequest)kvRequest).getDurability() != null) {
                    oneTimeMessage("The requested feature is not supported " +
                                   "by the connected server: Durability");
                }
            }

            if (serialVersion < 3 && kvRequest instanceof TableRequest) {
                TableLimits limits = ((TableRequest)kvRequest).getTableLimits();
                if (limits != null &&
                    limits.getMode() == CapacityMode.ON_DEMAND) {
                    oneTimeMessage("The requested feature is not supported " +
                                   "by the connected server: on demand " +
                                   "capacity table");
                }
            }

            ResponseHandler responseHandler = null;
            short serialVersionUsed = serialVersion;
            ByteBuf buffer = null;
            long networkLatency;
            try {
                /*
                 * NOTE: the ResponseHandler will release the Channel
                 * in its close() method, which is always called in the
                 * finally clause. This handles both successful and retried
                 * operations in the loop.
                 */
                Channel channel = httpClient.getChannel(thisIterationTimeoutMs);
                requestId = Long.toString(nextRequestId());
                responseHandler =
                    new ResponseHandler(httpClient, logger, channel,
                                        requestId, kvRequest.shouldRetry());
                buffer = channel.alloc().directBuffer();
                buffer.retain();

                /*
                 * we expressly check size limit below based on onprem versus
                 * cloud. Set the request to not check size limit inside
                 * writeContent().
                 */
                kvRequest.setCheckRequestSize(false);

                /* update timeout in request to match this iteration timeout */
                kvRequest.setTimeoutInternal(thisIterationTimeoutMs);

                serialVersionUsed = writeContent(buffer, kvRequest);

                /*
                 * If on-premises the authProvider will always be a
                 * StoreAccessTokenProvider. If so, check against
                 * configurable limit. Otherwise check against internal
                 * hardcoded cloud limit.
                 */
                if (authProvider instanceof StoreAccessTokenProvider) {
                    if (buffer.readableBytes() >
                        httpClient.getMaxContentLength()) {
                        throw new RequestSizeLimitException("The request " +
                            "size of " + buffer.readableBytes() +
                            " exceeded the limit of " +
                            httpClient.getMaxContentLength());
                    }
                } else {
                    kvRequest.setCheckRequestSize(true);
                    BinaryProtocol.checkRequestSizeLimit(
                        kvRequest, buffer.readableBytes());
                }

                final FullHttpRequest request =
                    new DefaultFullHttpRequest(HTTP_1_1, POST, kvRequestURI,
                                               buffer,
                                               false /* Don't validate hdrs */);
                HttpHeaders headers = request.headers();
                addCommonHeaders(headers);
                int contentLength = buffer.readableBytes();
                headers.add(HttpHeaderNames.HOST, host)
                    .add(REQUEST_ID_HEADER, requestId)
                    .setInt(CONTENT_LENGTH, contentLength);
                if (sessionCookie != null) {
                    headers.add(COOKIE, sessionCookie);
                }

                String serdeVersion = getSerdeVersion(kvRequest);
                if (serdeVersion != null) {
                    headers.add("x-nosql-serde-version", serdeVersion);
                }

                /*
                 * If the request doesn't set an explicit compartment, use
                 * the config default if provided.
                 */
                if (kvRequest.getCompartment() == null) {
                    kvRequest.setCompartmentInternal(
                        config.getDefaultCompartment());
                }
                authProvider.setRequiredHeaders(authString, kvRequest, headers);

                if (config.getDefaultNamespace() != null) {
                    headers.add(REQUEST_NAMESPACE_HEADER,
                                config.getDefaultNamespace());
                }

                if (isLoggable(logger, Level.FINE) &&
                    !kvRequest.getIsRefresh()) {
                    logTrace(logger, "Request: " + requestClass +
                                     ", requestId=" + requestId);
                }
                networkLatency = System.currentTimeMillis();
                httpClient.runRequest(request, responseHandler, channel);

                boolean isTimeout =
                    responseHandler.await(thisIterationTimeoutMs);
                if (isTimeout) {
                    throw new TimeoutException("Request timed out after " +
                        timeoutMs + " milliseconds: requestId=" + requestId);
                }

                if (isLoggable(logger, Level.FINE) &&
                    !kvRequest.getIsRefresh()) {
                    logTrace(logger, "Response: " + requestClass +
                                     ", status=" +
                                     responseHandler.getStatus() +
                                     ", requestId=" + requestId );
                }

                ByteBuf wireContent = responseHandler.getContent();
                Result res = processResponse(responseHandler.getStatus(),
                                       responseHandler.getHeaders(),
                                       wireContent,
                                       kvRequest);
                rateDelayedMs += getRateDelayedFromHeader(
                                       responseHandler.getHeaders());
                int resSize = wireContent.readerIndex();
                networkLatency = System.currentTimeMillis() - networkLatency;

                if (serialVersionUsed < 3) {
                    /* so we can emit a one-time message if the app */
                    /* tries to access modificationTime */
                    if (res instanceof GetResult) {
                        ((GetResult)res).setClient(this);
                    } else if (res instanceof WriteResult) {
                        ((WriteResult)res).setClient(this);
                    }
                }

                if (res instanceof TableResult && rateLimiterMap != null) {
                    /* update rate limiter settings for table */
                    TableLimits tl = ((TableResult)res).getTableLimits();
                    updateRateLimiters(((TableResult)res).getTableName(), tl);
                }

                /*
                 * We may not have rate limiters yet because queries may
                 * not have a tablename until after the first request.
                 * So try to get rate limiters if we don't have them yet and
                 * this is a QueryRequest.
                 */
                if (rateLimiterMap != null && readLimiter == null) {
                    readLimiter = getQueryRateLimiter(kvRequest, true);
                }
                if (rateLimiterMap != null && writeLimiter == null) {
                    writeLimiter = getQueryRateLimiter(kvRequest, false);
                }

                /* consume rate limiter units based on actual usage */
                rateDelayedMs += consumeLimiterUnits(readLimiter,
                                    res.getReadUnitsInternal(),
                                    thisIterationTimeoutMs);
                rateDelayedMs += consumeLimiterUnits(writeLimiter,
                                    res.getWriteUnitsInternal(),
                                    thisIterationTimeoutMs);
                res.setRateLimitDelayedMs(rateDelayedMs);

                /* copy retry stats to Result on successful operation */
                res.setRetryStats(kvRequest.getRetryStats());
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);

                statsControl.observe(kvRequest, Math.toIntExact(networkLatency),
                    contentLength, resSize);

                checkAuthRefreshList(kvRequest);

                return res;

            } catch (AuthenticationException rae) {
                if (authProvider != null &&
                    authProvider instanceof StoreAccessTokenProvider) {
                    final StoreAccessTokenProvider satp =
                        (StoreAccessTokenProvider) authProvider;
                    satp.bootstrapLogin();
                    kvRequest.addRetryException(rae.getClass());
                    kvRequest.incrementRetries();
                    exception = rae;
                    continue;
                }
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                statsControl.observeError(kvRequest);
                logInfo(logger, "Unexpected authentication exception: " +
                        rae);
                throw new NoSQLException("Unexpected exception: " +
                        rae.getMessage(), rae);
            } catch (InvalidAuthorizationException iae) {
                /*
                 * Allow a single retry for invalid/expired auth
                 * This includes "clock skew" errors
                 * This does not include permissions-related errors
                 */
                if (kvRequest.getNumRetries() > 0) {
                    /* same as NoSQLException below */
                    kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                    statsControl.observeError(kvRequest);
                    logFine(logger, "Client execute NoSQLException: " +
                            iae.getMessage());
                    throw iae;
                }
                /* flush auth cache and do one retry */
                authProvider.flushCache();
                kvRequest.addRetryException(iae.getClass());
                kvRequest.incrementRetries();
                exception = iae;
                logFine(logger,
                        "Client retrying on InvalidAuthorizationException: " +
                        iae.getMessage());
                continue;
            } catch (SecurityInfoNotReadyException sinre) {
                kvRequest.addRetryException(sinre.getClass());
                exception = sinre;
                int delayMs = SEC_ERROR_DELAY_MS;
                if (kvRequest.getNumRetries() > 10) {
                    delayMs =
                        DefaultRetryHandler.computeBackoffDelay(kvRequest, 0);
                    if (delayMs <= 0) {
                        break;
                    }
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {}
                kvRequest.incrementRetries();
                kvRequest.addRetryDelayMs(delayMs);
                continue;
            } catch (RetryableException re) {

                if (re instanceof WriteThrottlingException &&
                    writeLimiter != null) {
                    /* ensure we check write limits next loop */
                    checkWriteUnits = true;
                    /* set limiter to its limit, if not over already */
                    if (writeLimiter.getCurrentRate() < 100.0) {
                        writeLimiter.setCurrentRate(100.0);
                    }
                    /* call retry handler to manage sleep/delay */
                }
                if (re instanceof ReadThrottlingException &&
                    readLimiter != null) {
                    /* ensure we check read limits next loop */
                    checkReadUnits = true;
                    /* set limiter to its limit, if not over already */
                    if (readLimiter.getCurrentRate() < 100.0) {
                        readLimiter.setCurrentRate(100.0);
                    }
                    /* call retry handler to manage sleep/delay */
                }

                logFine(logger, "Retryable exception: " +
                        re.getMessage());
                /*
                 * Handle automatic retries. If this does not throw an error,
                 * then the delay (if any) will have been performed and the
                 * request should be retried.
                 *
                 * If there have been too many retries this method will
                 * throw the original exception.
                 */

                kvRequest.addRetryException(re.getClass());
                handleRetry(re, kvRequest);
                kvRequest.incrementRetries();
                exception = re;
                continue;
            } catch (UnsupportedProtocolException upe) {
                /* reduce protocol version and try again */
                if (decrementSerialVersion(serialVersionUsed) == true) {
                    exception = upe;
                    logFine(logger, "Got unsupported protocol error " +
                            "from server: decrementing serial version to " +
                            serialVersion + " and trying again.");
                    continue;
                }
                throw upe;
            } catch (NoSQLException nse) {
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                statsControl.observeError(kvRequest);
                logFine(logger, "Client execute NoSQLException: " +
                        nse.getMessage());
                throw nse; /* pass through */
            } catch (RuntimeException e) {
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                statsControl.observeError(kvRequest);
                if (!kvRequest.getIsRefresh()) {
                    /* don't log expected failures from refresh */
                    logFine(logger, "Client execute runtime exception: " +
                            e.getMessage());
                }
                throw e;
            } catch (IOException ioe) {
                /* Maybe make this logFine */
                String name = ioe.getClass().getName();
                logInfo(logger, "Client execution IOException, name: " +
                        name + ", message: " + ioe.getMessage());
                /*
                 * An exception in the channel, e.g. the server may have
                 * disconnected. Retry.
                 */
                kvRequest.addRetryException(ioe.getClass());
                kvRequest.incrementRetries();
                exception = ioe;

                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {}

                continue;
            } catch (InterruptedException ie) {
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                statsControl.observeError(kvRequest);
                logInfo(logger, "Client interrupted exception: " +
                        ie.getMessage());
                /* this exception shouldn't retry -- direct throw */
                throw new NoSQLException("Request interrupted: " +
                                         ie.getMessage());
            } catch (ExecutionException ee) {
                kvRequest.setRateLimitDelayedMs(rateDelayedMs);
                statsControl.observeError(kvRequest);
                logInfo(logger, "Unable to execute request: " +
                        ee.getCause().getMessage());
                /* is there a better exception? */
                throw new NoSQLException(
                    "Unable to execute request: " + ee.getCause().getMessage());
            } catch (TimeoutException te) {
                exception = te;
                logInfo(logger, "Timeout exception: " + te);
                break; /* fall through to exception below */
            } catch (Throwable t) {
                /*
                 * this is likely an exception from Netty, perhaps a bad
                 * connection. Retry.
                 */
                /* Maybe make this logFine */
                String name = t.getClass().getName();
                logInfo(logger, "Client execute Throwable, name: " +
                        name + "message: " + t.getMessage());

                kvRequest.addRetryException(t.getClass());
                kvRequest.incrementRetries();
                exception = t;
                continue;
            } finally {
                /*
                 * Because the buffer.retain() is called after initialized, so
                 * the reference count of buffer should be always > 0 here, just
                 * call buffer.release(refCnt) to release it.
                 */
                if (buffer != null) {
                    buffer.release(buffer.refCnt());
                }
                if (responseHandler != null) {
                    responseHandler.close();
                }
            }
        } while (! timeoutRequest(startTime, timeoutMs, exception));

        kvRequest.setRateLimitDelayedMs(rateDelayedMs);
        statsControl.observeError(kvRequest);
        throw new RequestTimeoutException(timeoutMs,
            requestClass + " timed out: requestId=" + requestId + " " +
            " nextRequestId=" + nextRequestId() +
            " iterationTimeout=" + thisIterationTimeoutMs + "ms " +
            (kvRequest.getRetryStats() != null ?
                kvRequest.getRetryStats() : ""), exception);
    }

    /**
     * Returns a rate limiter for a query operation, if the query op has
     * a prepared statement and a limiter exists in the rate limiter map
     * for the query table.
     */
    private RateLimiter getQueryRateLimiter(Request request, boolean read) {
        if (rateLimiterMap == null || !(request instanceof QueryRequest)) {
            return null;
        }

        /*
         * If we're asked for a write limiter, and the request doesn't
         * do writes, return null
         */
        if (read == false && ((QueryRequest)request).doesWrites() == false) {
            return null;
        }

        /*
         * We sometimes may only get a prepared statement after the
         * first query response is returned. In this case, we can get
         * the tablename from the request and apply rate limiting.
         */
        String tableName = ((QueryRequest)request).getTableName();
        if (tableName == null || tableName == "") {
            return null;
        }

        if (read) {
            RateLimiter rl = rateLimiterMap.getReadLimiter(tableName);
            if (rl != null) {
                request.setReadRateLimiter(rl);
            }
            return rl;
        }

        RateLimiter rl = rateLimiterMap.getWriteLimiter(tableName);
        if (rl != null) {
            request.setWriteRateLimiter(rl);
        }
        return rl;
    }

    /**
     * Consume rate limiter units after successful operation.
     * @return the number of milliseconds delayed due to rate limiting
     */
    private int consumeLimiterUnits(RateLimiter rl,
                                    long units, int timeoutMs) {

        if (rl == null || units <= 0) {
            return 0;
        }

        /*
         * The logic consumes units (and potentially delays) _after_ a
         * successful operation for a couple reasons:
         * 1) We don't know the actual number of units an op uses until
         *    after the operation successfully finishes
         * 2) Delaying after the op keeps the application from immediately
         *    trying the next op and ending up waiting along with other
         *    client threads until the rate goes below the limit, at which
         *    time all client threads would continue at once. By waiting
         *    after a successful op, client threads will get staggered
         *    better to avoid spikes in throughput and oscillation that
         *    can result from it.
         */

        try {
            return rl.consumeUnitsWithTimeout(units, timeoutMs, false);
        } catch (TimeoutException e) {
            /* Don't throw - operation succeeded. Just return timeoutMs. */
            return timeoutMs;
        }
    }


    /**
     * Add or update rate limiters for a table.
     * Cloud only.
     *
     * @param tableName table name or OCID of table
     * @param limits read/write limits for table
     */
    public boolean updateRateLimiters(String tableName, TableLimits limits) {
        if (rateLimiterMap == null) {
            return false;
        }

        setTableNeedsRefresh(tableName, false);

        if (limits == null ||
            (limits.getReadUnits() <= 0 && limits.getWriteUnits() <= 0)) {
            rateLimiterMap.remove(tableName);
            logInfo(logger, "removing rate limiting from table " + tableName);
            return false;
        }

        /*
         * Create or update rate limiters in map
         * Note: noSQL cloud service has a "burst" availability of
         * 300 seconds. But we don't know if or how many other clients
         * may have been using this table, and a duration of 30 seconds
         * allows for more predictable usage. Also, it's better to
         * use a reasonable hardcoded value here than to try to explain
         * the subtleties of it in docs for configuration. In the end
         * this setting is probably fine for all uses.
         */

        /* allow tests to override this hardcoded setting */
        int durationSeconds = Integer.getInteger("test.rldurationsecs", 30)
                                     .intValue();

        double RUs = (double)limits.getReadUnits();
        double WUs = (double)limits.getWriteUnits();

        /* if there's a specified rate limiter percentage, use that */
        double rlPercent = config.getDefaultRateLimitingPercentage();
        if (rlPercent > 0.0) {
            RUs = (RUs * rlPercent) / 100.0;
            WUs = (WUs * rlPercent) / 100.0;
        }

        rateLimiterMap.update(tableName, RUs, WUs, durationSeconds);
        final String msg = String.format("Updated table '%s' to have " +
            "RUs=%.1f and WUs=%.1f per second", tableName, RUs, WUs);
        logInfo(logger, msg);

        return true;
    }


    /**
     * Determine if the request should be timed out.
     * Check if the request exceed the timeout given.
     *
     * @param startTime when the request starts
     * @param requestTimeout the default timeout of this request
     * @param exception the last exception
     *
     * @return true the request need to be timed out.
     */
    boolean timeoutRequest(long startTime,
                           long requestTimeout,
                           Throwable exception) {
        return ((System.currentTimeMillis() - startTime) >= requestTimeout);
    }

    /**
     * Serializes the request payload, sent as http content
     *
     * @param content the buffer to contain the content
     *
     * @throws IOException
     */
    private short writeContent(ByteBuf content, Request kvRequest)
        throws IOException {

        final NettyByteOutputStream bos = new NettyByteOutputStream(content);
        final short versionUsed = serialVersion;
        SerializerFactory factory = chooseFactory(kvRequest);
        factory.writeSerialVersion(versionUsed, bos);
        kvRequest.createSerializer(factory).serialize(kvRequest,
                                                      versionUsed,
                                                      bos);
        return versionUsed;
    }

    /**
     * Processes the httpResponse object converting it into a suitable
     * return value.
     *
     * @param content the response from the service
     *
     * @return the programmatic response object
     */
    final Result processResponse(HttpResponseStatus status,
                                 HttpHeaders headers,
                                 ByteBuf content,
                                 Request kvRequest) {

        if (!HttpResponseStatus.OK.equals(status)) {
            processNotOKResponse(status, content);

            /* TODO: Generate and handle bad status other than 400 */
            throw new IllegalStateException("Unexpected http response status:" +
                                            status);
        }

        setSessionCookie(headers);

        try (ByteInputStream bis = new NettyByteInputStream(content)) {
            return processOKResponse(bis, kvRequest);
        }
    }

    /**
     * Process an OK response
     *
     * @return the result of processing the successful request
     *
     * @throws NoSQLException if the stream could not be read for some reason
     */
    Result processOKResponse(ByteInputStream in, Request kvRequest) {
        try {
            SerializerFactory factory = chooseFactory(kvRequest);
            int code = factory.readErrorCode(in);
            /* note: this will always be zero in V4 */
            if (code == 0) {
                Result res =
                    kvRequest.createDeserializer(factory).
                    deserialize(kvRequest,
                                in,
                                serialVersion);

                if (kvRequest.isQueryRequest()) {
                    QueryRequest qreq = (QueryRequest)kvRequest;
                    if (!qreq.isSimpleQuery()) {
                        qreq.getDriver().setClient(this);
                    }
                }
                return res;
            }
            /*
             * Operation failed. Handle the failure and throw an appropriate
             * exception.
             */
            String err = readString(in);

            /* special case for TNF errors on WriteMultiple with many tables */
            if (code == TABLE_NOT_FOUND &&
                (kvRequest instanceof WriteMultipleRequest)) {
                throw handleWriteMultipleTableNotFound(code, err,
                                     (WriteMultipleRequest)kvRequest);
            }

            throw handleResponseErrorCode(code, err);
        } catch (IOException e) {
            e.printStackTrace();
            /*
             * TODO: Retrying here will not actually help, the
             * operation should be abandoned; we need a specific
             * exception to indicate this
             */
            throw new NoSQLException(e.getMessage());
        }
    }


    /**
     * Process NotOK response. The method typically throws an appropriate
     * exception. A normal return indicates that the method declined to
     * handle the response and it's the caller's responsibility to take
     * appropriate action.
     *
     * @param status the http response code it must not be OK
     *
     * @param payload the payload representing the failure response
     */
    private void processNotOKResponse(HttpResponseStatus status,
                                      ByteBuf payload) {
        if (HttpResponseStatus.BAD_REQUEST.equals(status)) {
            int len = payload.readableBytes();
            String errMsg = (len > 0)?
                payload.readCharSequence(len, UTF_8).toString() :
                status.reasonPhrase();
            throw new NoSQLException("Error response: " + errMsg);
        }
        throw new NoSQLException("Error response = " + status +
                                 ", reason = " + status.reasonPhrase());
    }

    /* set session cookie, if set in response headers */
    private void setSessionCookie(HttpHeaders headers) {
        if (headers == null) {
            return;
        }
        /*
         * NOTE: this code assumes there will always be at most
         * one Set-Cookie header in the response. If the load balancer
         * settings change, or the proxy changes to add Set-Cookie
         * headers, this code may need to be changed to look for
         * multiple Set-Cookie headers.
         */
        String v = headers.get("Set-Cookie");
        /* note SESSION_COOKIE_FIELD has appended '=' */
        if (v == null || v.startsWith(SESSION_COOKIE_FIELD) == false) {
            return;
        }
        int semi = v.indexOf(";");
        if (semi < 0) {
            setSessionCookieValue(v);
        } else {
            setSessionCookieValue(v.substring(0, semi));
        }
        if (isLoggable(logger, Level.FINE)) {
            logTrace(logger, "Set session cookie to \"" + sessionCookie + "\"");
        }
    }

    private synchronized void setSessionCookieValue(String pVal) {
        sessionCookie = pVal;
    }

    /**
     * Return true if table needs limits refresh.
     */
    private boolean tableNeedsRefresh(String tableName) {
        if (tableLimitUpdateMap == null) {
            return false;
        }

        AtomicLong then = tableLimitUpdateMap.get(tableName);
        long nowNanos = System.nanoTime();
        if (then != null && then.get() > nowNanos) {
            return false;
        }
        return true;
    }

    /**
     * set the status of a table needing limits refresh now
     */
    private void setTableNeedsRefresh(String tableName, boolean needsRefresh) {
        if (tableLimitUpdateMap == null) {
            return;
        }

        AtomicLong then = tableLimitUpdateMap.get(tableName);
        long nowNanos = System.nanoTime();
        if (then != null) {
            if (needsRefresh == false) {
                then.set(nowNanos + LIMITER_REFRESH_NANOS);
            } else {
                then.set(nowNanos - 1);
            }
            return;
        }

        if (needsRefresh == true) {
            tableLimitUpdateMap.put(tableName, new AtomicLong(nowNanos - 1));
        } else {
            tableLimitUpdateMap.put(tableName,
                new AtomicLong(nowNanos + LIMITER_REFRESH_NANOS));
        }
    }

    /**
     * Query table limits and create rate limiters for a table in a
     * short-lived background thread.
     */
    private synchronized void backgroundUpdateLimiters(String tableName,
                                                       String compartmentId) {
        if (tableNeedsRefresh(tableName) == false) {
            return;
        }
        setTableNeedsRefresh(tableName, false);

        try {
            threadPool.execute(() -> {
                updateTableLimiters(tableName, compartmentId);
            });
        } catch (RejectedExecutionException e) {
            setTableNeedsRefresh(tableName, true);
        }
    }

    /*
     * This is meant to be run in a background thread
     */
    private void updateTableLimiters(String tableName, String compartmentId) {

        GetTableRequest gtr = new GetTableRequest()
            .setTableName(tableName)
            .setCompartment(compartmentId)
            .setTimeout(1000);
        TableResult res = null;
        try {
            logInfo(logger, "Starting GetTableRequest for table '" +
                tableName + "'");
            res = (TableResult) this.execute(gtr);
        } catch (Exception e) {
            logInfo(logger, "GetTableRequest for table '" +
                tableName + "' returned exception: " + e.getMessage());
        }

        if (res == null) {
            /* table doesn't exist? other error? */
            logInfo(logger, "GetTableRequest for table '" +
                tableName + "' returned null");
            AtomicLong then = tableLimitUpdateMap.get(tableName);
            if (then != null) {
                /* allow retry after 100ms */
                then.set(System.nanoTime() + 100_000_000L);
            }
            return;
        }

        logInfo(logger, "GetTableRequest completed for table '" +
            tableName + "'");
        /* update/add rate limiters for table */
        if (updateRateLimiters(tableName, res.getTableLimits())) {
            logInfo(logger, "background thread added limiters for table '" +
                tableName + "'");
        }
    }


    private void handleRetry(RetryableException re,
                            Request kvRequest) {
        int numRetries = kvRequest.getNumRetries();
        String msg = "Retry for request " +
            kvRequest.getClass().getSimpleName() + ", num retries: " +
            numRetries + ", exception: " + re.getMessage();
        logFine(logger, msg);
        RetryHandler handler = config.getRetryHandler();
        if (!handler.doRetry(kvRequest, numRetries, re)) {
            logFine(logger, "Too many retries");
            throw re;
        }
        handler.delay(kvRequest, numRetries, re);
    }

    private void logRetries(int numRetries, Throwable exception) {
        Level level = Level.FINE;
        if (logger != null) {
            logger.log(level, "Client, doing retry: " + numRetries +
                       (exception != null ? ", exception: " + exception : ""));
        }
    }

    private String readString(ByteInputStream in) throws IOException {
            return SerializationUtil.readString(in);
    }

    /**
     * Map a specific error status to a specific exception.
     */
    private RuntimeException handleResponseErrorCode(int code, String msg) {
        RuntimeException exc = BinaryProtocol.mapException(code, msg);
        throw exc;
    }

    /*
     * special case for TNF errors on WriteMultiple with many tables.
     * Earlier server versions do not support this and will return a
     * Table Not Found error with the table names in a single string,
     * separated by commas, with no brackets, like:
     *    table1,table2,table3
     *
     * Later versions may legitimately return Table Not Found,
     * but table names will be inside a bracketed list, like:
     *    [table1, table2, table3]
     */
    private RuntimeException handleWriteMultipleTableNotFound(int code,
                                 String msg,
                                 WriteMultipleRequest wrRequest) {
        if (code != TABLE_NOT_FOUND ||
            wrRequest.isSingleTable() ||
            msg.indexOf(",") < 0 ||
            msg.indexOf("[") >= 0) {
            return handleResponseErrorCode(code, msg);
        }
        throw new OperationNotSupportedException("WriteMultiple requests " +
                      "using multiple tables are not supported by the " +
                      "version of the connected server.");
    }

    private void addCommonHeaders(HttpHeaders headers) {
        headers.set(CONTENT_TYPE, "application/octet-stream")
            .set(CONNECTION, "keep-alive")
            .set(ACCEPT, "application/octet-stream")
            .set(USER_AGENT, getUserAgent());
    }

    private String getUserAgent() {
        return userAgent;
    }

    public static void trace(String msg, int level) {
        if (level <= traceLevel) {
            System.out.println("DRIVER: " + msg);
        }
    }

    /**
     * @hidden
     *
     * Allow tests to reset limiters in map
     *
     * @param tableName name or OCID of the table
     */
    public void resetRateLimiters(String tableName) {
        if (rateLimiterMap != null) {
            rateLimiterMap.reset(tableName);
        }
    }

    /**
     * @hidden
     *
     * Allow tests to enable/disable rate limiting
     * This method is not thread safe, and should only be
     * executed by one thread when no other operations are
     * in progress.
     */
    public void enableRateLimiting(boolean enable, double usePercent) {
        config.setDefaultRateLimitingPercentage(usePercent);
        if (enable == true && rateLimiterMap == null) {
            rateLimiterMap = new RateLimiterMap();
            tableLimitUpdateMap = new ConcurrentHashMap<String, AtomicLong>();
            threadPool = Executors.newSingleThreadExecutor();
        } else if (enable == false && rateLimiterMap != null) {
            rateLimiterMap.clear();
            rateLimiterMap = null;
            tableLimitUpdateMap.clear();
            tableLimitUpdateMap = null;
            if (threadPool != null) {
                threadPool.shutdown();
                threadPool = null;
            }
        }
    }

    /**
     * Returns the statistics control object.
     */
    StatsControl getStatsControl() {
        return statsControl;
    }

    /**
     * @hidden
     *
     * Try to decrement the serial protocol version.
     * @return true: version was decremented
     *         false: already at lowest version number.
     */
    private synchronized boolean decrementSerialVersion(short versionUsed) {
        if (serialVersion != versionUsed) {
            return true;
        }
        if (serialVersion == V4) {
            serialVersion = V3;
            return true;
        }
        if (serialVersion == V3) {
            serialVersion = V2;
            return true;
        }
        return false;
    }

    /**
     * @hidden
     * For testing use
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * @hidden
     * For testing use
     */
    public void setSerialVersion(short version) {
        serialVersion = version;
    }

    /**
     * tell the Client to track auth refresh requests. This will be called
     * in a cloud service configuration
     */
    void createAuthRefreshList() {
        authRefreshRequests = new ConcurrentLinkedQueue<Request>();
        badValue = new MapValue().put("@bad", 0);
    }

    /**
     * Run refresh requests
     */
    void doRefresh(long refreshMs) {
        final long minMsPerRequest = 20L; // this is somewhat arbitrary
        int numRequests = authRefreshRequests.size();
        if (numRequests == 0) {
            return;
        }

        /*
         * Divide the total time allowed (refreshMs) by the number of requests
         * (x 3) to determine a reasonable timeout for each request. A timed out
         * request still does the re-auth assuming it made it to the server, but
         * can result in creating/consuming additional communication channels
         */
        int msPerRequest = (int) (refreshMs/(numRequests * 3));
        if (msPerRequest < minMsPerRequest) {
            logFine(logger,
                    "Not enough time per request to perform auth refresh. " +
                    "numRequests=" + numRequests + " totalMs=" + refreshMs +
                    " msPerRequest=" + msPerRequest);
            return;
        }

        logFine(logger,
                "Performing auth refresh. numRequests=" + numRequests +
                " refreshMs=" + refreshMs + "ms per request=" + msPerRequest);

        /*
         * Because there are 3 proxies and scheduling is round-robin
         * do this 3 times to attempt to catch all of them. Unfortunately
         * this heuristic will not work much of the time because the
         * round-robin scheduling isn't per-connection or IP address.
         * If there is a sessionCookie, session persistence is in play
         * and only one proxy needs to be refreshed.
         */
        final int numCallsPerRequest = sessionCookie != null ? 1 : 3;

        /* use iterator so that remove works more simply */
        Iterator<Request> iter = authRefreshRequests.iterator();
        while (iter.hasNext()) {
            Request rq = iter.next();
            /* set timeout calculated above */
            rq.setTimeoutInternal(msPerRequest);
            for (int i = 0; i < numCallsPerRequest; i++) {
                try {
                    execute(rq);
                } catch (TableNotFoundException tnf) {
                    /* table is gone -- remove from list */
                    logFine(logger, "Auth refresh table not found, removing: " +
                            rq.getClass().getSimpleName() + " for " +
                            rq.getTableName());
                    iter.remove();
                    break;
                } catch (Throwable th) {
                    /* ignore */
                }
            }
        }
    }

    /**
     * Look for the compartment,table combination in the list. If
     * present, do nothing. If not, add that combination to the list.
     * This is not particularly efficient but it is not expected that a given
     * handle will be accessing a large number of tables.
     * <p>
     * The operation type is not checked -- all 3 types of requests are created
     * no matter the access. This simplifies the logic and if a given type is
     * not given to this Principal it doesn't hurt.
     */
    private void checkAuthRefreshList(Request request) {
        if (authRefreshRequests != null) {
            /*
             * don't add refresh requests and those without table names can't be
             * added. They may be a query or admin op. Also eliminate ops that
             * don't read or write, which includes GetTableRequest
             */
            if (request.getIsRefresh() || request.getTableName() == null ||
                !(request.doesReads() || request.doesWrites())) {
                return;
            }
            for (Request rq : authRefreshRequests) {
                if (rq.getTableName().equalsIgnoreCase(request.getTableName()) &&
                    stringsEqualOrNull(rq.getCompartment(),
                                       request.getCompartment())) {
                    return;
                }
            }
            /* request compartment, table, type isn't present - add it */
            addRequestToRefreshList(request);
        }
    }

    private boolean stringsEqualOrNull(String s1, String s2) {
        if (s1 == null) {
            return s2 == null;
        }
        return s1.equalsIgnoreCase(s2);
    }

    /**
     * Add get, put, delete to cover all auth types
     * This is synchronized to avoid 2 requests adding the same table
     */
    private synchronized void addRequestToRefreshList(Request request) {
        logFine(logger, "Adding table to request list: " +
                request.getCompartment() + ":" + request.getTableName());
        PutRequest pr =
            new PutRequest().setTableName(request.getTableName());
        pr.setCompartmentInternal(request.getCompartment());
        pr.setValue(badValue);
        pr.setIsRefresh(true);
        authRefreshRequests.add(pr);
        GetRequest gr =
            new GetRequest().setTableName(request.getTableName());
        gr.setCompartmentInternal(request.getCompartment());
        gr.setKey(badValue);
        gr.setIsRefresh(true);
        authRefreshRequests.add(gr);
        DeleteRequest dr =
            new DeleteRequest().setTableName(request.getTableName());
        dr.setCompartmentInternal(request.getCompartment());
        dr.setKey(badValue);
        dr.setIsRefresh(true);
        authRefreshRequests.add(dr);
    }

    /**
     * @hidden
     * for internal use
     */
    public synchronized void oneTimeMessage(String msg) {
        if (oneTimeMessages.add(msg) == false) {
            return;
        }
        logWarning(logger, msg);
    }

    private SerializerFactory chooseFactory(Request rq) {
        if (serialVersion == 4) {
            return v4factory;
        } else {
            return v3factory; /* works for v2 also */
        }
    }

    private String getSerdeVersion(Request rq) {
        return chooseFactory(rq).getSerdeVersionString();
    }

    /*
     * If the response has a header indicating the amount of time the
     * server side delayed the request due to rate limiting, return that
     * value (in milliseconds).
     */
    private int getRateDelayedFromHeader(HttpHeaders headers) {
        if (headers == null) {
            return 0;
        }
        String v = headers.get(X_RATELIMIT_DELAY);
        if (v == null || v.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(v);
        } catch (Exception e) {
        }

        return 0;
    }

    /**
     * @hidden
     * For testing use
     */
    public void setDefaultNamespace(String ns) {
        config.setDefaultNamespace(ns);
    }
}
