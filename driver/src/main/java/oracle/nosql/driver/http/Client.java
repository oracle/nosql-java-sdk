/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import static io.netty.handler.codec.http.DefaultHttpHeadersFactory.headersFactory;
import static io.netty.handler.codec.http.DefaultHttpHeadersFactory.trailersFactory;
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
import static oracle.nosql.driver.util.HttpConstants.SERVER_SERIAL_VERSION;
import static oracle.nosql.driver.util.HttpConstants.USER_AGENT;
import static oracle.nosql.driver.util.HttpConstants.X_RATELIMIT_DELAY;
import static oracle.nosql.driver.util.LogUtil.isLoggable;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.LogUtil.logTrace;
import static oracle.nosql.driver.util.LogUtil.logWarning;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
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
import oracle.nosql.driver.UnsupportedQueryVersionException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.DurableRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
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
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ConcurrentUtil;
import oracle.nosql.driver.util.HttpConstants;
import oracle.nosql.driver.util.LogUtil;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.RateLimiterMap;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.util.SimpleRateLimiter;
import oracle.nosql.driver.values.MapValue;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;

/**
 * The HTTP driver client.
 */
public final class Client {

    public static int traceLevel = 0;
    private static final int cores = Runtime.getRuntime().availableProcessors();

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
    private final AtomicLong maxRequestId = new AtomicLong(1);

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
     * single thread executor for updating table limits
     */
    private ExecutorService threadPool;

    private volatile short serialVersion = DEFAULT_SERIAL_VERSION;

    /* separate version for query compatibility */
    private volatile short queryVersion = QueryDriver.QUERY_VERSION;

    /* for one-time messages */
    private final HashSet<String> oneTimeMessages;

    /**
     * config for statistics
     */
    private final StatsControlImpl statsControl;

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
    private final String userAgent;

    private volatile TopologyInfo topology;

    /* for internal testing */
    private final String prepareFilename;

    /* thread-pool for scheduling tasks */
    private final ScheduledExecutorService taskExecutor;

     /* Lock to access data structures */
    private final ReentrantLock lock = new ReentrantLock();

    /*
     * Centralized error handling for request execution.
     * This class maps specific {@link Throwable} types to error-handling
     * strategies (retry, fail, or protocol downgrade).
     * It uses a HashMap of {@link ErrorHandler} functions
     * to keep {@link #handleError(RequestContext, Throwable)} short and
     * maintainable.
     */
    private final Map<Class<? extends Throwable>, ErrorHandler>
        errorHandlers = new HashMap<>();

    /*
     * Functional interface for all error handlers.
     * Each handler inspects the exception and decides whether
     * to retry the request or fail with an exception.
     */
    @FunctionalInterface
    private interface ErrorHandler {
        CompletableFuture<Result> handle(RequestContext ctx, Throwable error);
    }

    /**
     * RequestContext class to encapsulate request-specific data.
     * This helps in passing context through asynchronous chains.
     * It now includes requestId and a Supplier to generate new IDs for retries.
     */
    private static class RequestContext {
        private final Request kvRequest;
        private final String requestClass;
        private volatile String requestId;
        private final long startNanos;
        private final int timeoutMs;
        private final Supplier<Long> nextIdSupplier;
        private volatile Throwable exception;
        private final AtomicInteger rateDelayedMs = new AtomicInteger(0);
        private volatile RateLimiter readLimiter;
        private volatile RateLimiter writeLimiter;
        private volatile boolean checkReadUnits;
        private volatile boolean checkWriteUnits;
        private volatile int reqSize;
        private volatile int resSize;
        private volatile short serialVersionUsed;
        private volatile short queryVersionUsed;
        private volatile long latencyNanos;
        public volatile long networkLatency;

        RequestContext(Request kvRequest, long startNanos, int timeoutMs,
                       Supplier<Long> nextIdSupplier, RateLimiter readLimiter,
                       RateLimiter writeLimiter, boolean checkReadUnits,
                       boolean checkWriteUnits) {
            this.kvRequest = kvRequest;
            this.startNanos = startNanos;
            this.timeoutMs = timeoutMs;
            this.nextIdSupplier = nextIdSupplier;
            this.readLimiter = readLimiter;
            this.writeLimiter = writeLimiter;
            this.checkReadUnits = checkReadUnits;
            this.checkWriteUnits = checkWriteUnits;

            this.requestId = Long.toString(nextIdSupplier.get());
            this.requestClass = kvRequest.getClass().getSimpleName();
        }
    }

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

        /* for internal testing */
        prepareFilename = System.getProperty("test.preparefilename");

        taskExecutor = new ScheduledThreadPoolExecutor(cores /* core threads */,
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    final Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName(String.format("nosql-task-executor-%s",
                        threadNumber.getAndIncrement()));
                    t.setDaemon(true);
                    t.setUncaughtExceptionHandler((thread, error) -> {
                        if (ConcurrentUtil.unwrapCompletionException(error)
                                instanceof RejectedExecutionException) {
                            /*
                             * Ignore uncaught error for rejected exception
                             * since that is expected to happen during
                             * executor shut down.
                             */
                            return;
                        }
                        logger.warning(() -> String.format(
                            "Uncaught exception from %s: %s",
                            error, LogUtil.getStackTrace(error)));
                    });
                    return t;
                }
            });
        initErrorHandlers();
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
        if (taskExecutor != null) {
            taskExecutor.shutdown();
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
    private long nextRequestId()  {
        return maxRequestId.addAndGet(1);
    }

    /**
     * Execute the KV request and return the future response. This is the
     * top-level method for request execution.
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
     * @return the future representing the result of the request
     */
    public CompletableFuture<Result> execute(Request kvRequest) {

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
        try {
            kvRequest.validate();
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }

        /* clear any retry stats that may exist on this request object */
        kvRequest.setRetryStats(null);
        kvRequest.setRateLimitDelayedMs(0);

        /* kvRequest.isQueryRequest() returns true if kvRequest is a
         * non-internal QueryRequest */
        if (kvRequest.isQueryRequest()) {

            QueryRequest qreq = (QueryRequest)kvRequest;

            /* Set the topo seq num in the request, if it has not been set
             * already */
            kvRequest.setTopoSeqNum(getTopoSeqNum());

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
                return CompletableFuture.completedFuture(
                    new QueryResult(qreq, false));
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
                return CompletableFuture.completedFuture(
                    new QueryResult(qreq, false));
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
            qreq.incBatchCounter();
        }

        /*
         * If the request doesn't set an explicit compartment, use
         * the config default if provided.
         */
        if (kvRequest.getCompartment() == null) {
            kvRequest.setCompartmentInternal(
                config.getDefaultCompartment());
        }

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

        kvRequest.setStartNanos(System.nanoTime());
        RequestContext ctx = new RequestContext(kvRequest,
            kvRequest.getStartNanos(), kvRequest.getTimeoutInternal(),
            this::nextRequestId, readLimiter, writeLimiter,
            checkReadUnits, checkWriteUnits);

        return executeWithRetry(ctx);
    }


    /*
     * Core method which creates the request and send to the server.
     * If the request fails, it performs retry.
     */
    private CompletableFuture<Result> executeWithRetry(RequestContext ctx) {

        final Request kvRequest = ctx.kvRequest;
        final int timeoutMs = ctx.timeoutMs;
        final long startNanos = ctx.startNanos;
        final int thisIterationTimeoutMs =
            getIterationTimeoutMs(timeoutMs, startNanos);

        /* Check for over all request timeout first */
        if (thisIterationTimeoutMs <= 0) {
            RequestTimeoutException rte = new RequestTimeoutException(timeoutMs,
                ctx.requestClass + " timed out:" +
                (ctx.requestId.isEmpty() ? "" : " requestId=" + ctx.requestId) +
                " nextRequestId=" + nextRequestId() +
                " iterationTimeout=" + thisIterationTimeoutMs + "ms " +
                (kvRequest.getRetryStats() != null ?
                kvRequest.getRetryStats() : ""), ctx.exception);
            return CompletableFuture.failedFuture(rte);
        }

        /* Log retry */
        if (kvRequest.getNumRetries() > 0) {
            logRetries(kvRequest.getNumRetries(), ctx.exception);
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

        return handlePreRateLimit(ctx)
        .thenCompose( (Integer delay) -> getAuthString(ctx, authProvider))
        .thenCompose((String authString) -> createRequest(ctx, authString))
        .thenCompose((FullHttpRequest request) -> submitRequest(ctx,request))
        .thenApply((FullHttpResponse response) -> handleResponse(ctx, response))
        .thenApply((Result result) -> handleResult(ctx, result))
        .thenCompose((Result result) -> handlePostRateLimit(ctx, result))
        .handle((Result result, Throwable err) -> {
            /* Handle error and retry */
            if (err != null) {
                return handleError(ctx, err);
            } else {
                return CompletableFuture.completedFuture(result);
            }
        })
        .thenCompose(Function.identity());
    }

    private CompletableFuture<Integer> handlePreRateLimit(RequestContext ctx) {
        /*
         * Check rate limiters before executing the request.
         * Wait for read and/or write limiters to be below their limits
         * before continuing. Be aware of the timeout given.
         */
        int preRateLimitDelayMs = 0;
        if (ctx.readLimiter != null && ctx.checkReadUnits) {
            preRateLimitDelayMs += ((SimpleRateLimiter) ctx.readLimiter)
                .consumeExternally(0);
        }
        if (ctx.writeLimiter != null && ctx.checkWriteUnits) {
            preRateLimitDelayMs += ((SimpleRateLimiter) ctx.writeLimiter)
                .consumeExternally(0);
        }

        int thisIterationTimeoutMs =
            getIterationTimeoutMs(ctx.timeoutMs, ctx.startNanos);

        /* If rate limit result in timeout, complete with exception. */
        if (thisIterationTimeoutMs <= preRateLimitDelayMs) {
            final TimeoutException ex = new TimeoutException(
                "timed out waiting "
                + thisIterationTimeoutMs
                + "ms due to rate limiting");
            return createDelayFuture(thisIterationTimeoutMs)
                .thenCompose(d -> CompletableFuture.failedFuture(ex));
        }
        /* sleep for delay ms */
        return createDelayFuture(preRateLimitDelayMs)
            .whenComplete((delay, err) -> ctx.rateDelayedMs.addAndGet(delay));
    }

    /**
     *  Get auth token from auth provider.
     *  This may contact the server to get the token.
     */
    private CompletableFuture<String> getAuthString(RequestContext ctx,
                                        AuthorizationProvider authProvider) {
        final Request kvRequest = ctx.kvRequest;
        return authProvider.getAuthorizationStringAsync(kvRequest)
        .thenApply(authString -> {
            /* Check whether timed out while acquiring the auth token */
            if (timeoutRequest(kvRequest.getStartNanos(),
                kvRequest.getTimeoutInternal(), null)) {
                TimeoutException ex = new TimeoutException(
                    "timed out during auth token acquisition");
                throw new CompletionException(ex);
            }
            /* validate the token is valid or not */
            authProvider.validateAuthString(authString);
            return authString;
        });
    }
    /**
     *  Create Netty HTTP request.
     *  This will serialize the request body and fill the HTTP headers and
     *  body.
     *  This may contact the server to sign the request body.
     */
    private CompletableFuture<FullHttpRequest> createRequest(RequestContext ctx,
                                                            String authString) {
        ByteBuf buffer = null;
        try {
            buffer = Unpooled.buffer();
            final Request kvRequest = ctx.kvRequest;
            /*
             * we expressly check size limit below based on onprem versus
             * cloud. Set the request to not check size limit inside
             * writeContent().
             */
            kvRequest.setCheckRequestSize(false);

            /* Set the topo seq num in the request, if it has not been set
             * already */
            if (!(kvRequest instanceof QueryRequest) ||
                    kvRequest.isQueryRequest()) {
                kvRequest.setTopoSeqNum(getTopoSeqNum());
            }

            /*
             * Temporarily change the timeout in the request object so
             * the serialized timeout sent to the server is correct for
             * this iteration. After serializing the request, set the
             * timeout back to the overall request timeout so that other
             * processing (retry delays, etc) work correctly.
             */
            kvRequest.setTimeoutInternal(
                getIterationTimeoutMs(ctx.timeoutMs, ctx.startNanos));
            writeContent(buffer, ctx);
            kvRequest.setTimeoutInternal(ctx.timeoutMs);

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
                new DefaultFullHttpRequest(
                    HTTP_1_1, POST, kvRequestURI,
                    buffer,
                    headersFactory().withValidation(false),
                    trailersFactory().withValidation(false));
            HttpHeaders headers = request.headers();
            addCommonHeaders(headers);
            int contentLength = buffer.readableBytes();
            ctx.reqSize = contentLength;
            headers.add(HttpHeaderNames.HOST, host)
                .add(REQUEST_ID_HEADER, ctx.requestId)
                .setInt(CONTENT_LENGTH, contentLength);
            if (sessionCookie != null) {
                headers.add(COOKIE, sessionCookie);
            }
            String serdeVersion = getSerdeVersion(kvRequest);
            if (serdeVersion != null) {
                headers.add("x-nosql-serde-version", serdeVersion);
            }

            /*
             * boolean that indicates whether content must be signed. Cross
             * region operations must include content when signing. See comment
             * on the method
             */
            final boolean signContent = requireContentSigned(kvRequest);

            /*
             * Get request body bytes if the request needed to be signed
             * with content
             */
            byte[] content = signContent ? getBodyBytes(buffer) : null;
            return authProvider.
            setRequiredHeadersAsync(authString, kvRequest, headers, content)
            .thenApply(n -> {
                String namespace = kvRequest.getNamespace();
                if (namespace == null) {
                    namespace = config.getDefaultNamespace();
                }
                if (namespace != null) {
                    headers.add(REQUEST_NAMESPACE_HEADER, namespace);
                }
                return request;
            });
        } catch (Throwable e) {
            /* Release the buffer on error */
            if (buffer != null) {
                buffer.release();
            }
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Send the HTTP request to server and get the response back.
     */
    private CompletableFuture<FullHttpResponse> submitRequest(RequestContext ctx,
                                                          HttpRequest request) {
        final Request kvRequest = ctx.kvRequest;
        if (isLoggable(logger, Level.FINE) && !kvRequest.getIsRefresh()) {
            logTrace(logger, "Request: " + ctx.requestClass +
                    ", requestId=" + ctx.requestId);
        }
        ctx.latencyNanos = System.nanoTime();
        int timeoutMs = getIterationTimeoutMs(ctx.timeoutMs, ctx.startNanos);

        return httpClient.runRequest(request, timeoutMs)
        .whenComplete((res, err) -> {
            ctx.networkLatency =
                (System.nanoTime() - ctx.latencyNanos) / 1_000_000;
        });
    }

    /**
     * Deserialize HTTP response into NoSQL Result.
     */
    private Result handleResponse(RequestContext ctx, FullHttpResponse fhr) {
        final Request kvRequest = ctx.kvRequest;
        if (isLoggable(logger, Level.FINE) && !kvRequest.getIsRefresh()) {
            logTrace(logger, "Response: " + ctx.requestClass +
                ", status=" + fhr.status() +
                ", requestId=" + ctx.requestId );
        }
        try {
            Result result = processResponse(
                fhr.status(), fhr.headers(), fhr.content(), ctx);
            ctx.rateDelayedMs.addAndGet(
                getRateDelayedFromHeader(fhr.headers()));
            ctx.resSize = fhr.content().readerIndex();
            return result;
        } finally {
            fhr.release(); //release response
        }
    }

    /**
     * Update stats from the result.
     */
    private Result handleResult(RequestContext ctx, Result result) {
        final Request kvRequest = ctx.kvRequest;
        setTopology(result.getTopology());
        if (ctx.serialVersionUsed < 3) {
            /* so we can emit a one-time message if the app */
            /* tries to access modificationTime */
            if (result instanceof GetResult) {
                ((GetResult)result).setClient(this);
            } else if (result instanceof WriteResult) {
                ((WriteResult)result).setClient(this);
            }
        }
        if (result instanceof QueryResult && kvRequest.isQueryRequest()) {
            QueryRequest qreq = (QueryRequest)kvRequest;
            qreq.addQueryTraces(((QueryResult)result).getQueryTraces());
        }
        if (result instanceof TableResult && rateLimiterMap != null) {
            /* update rate limiter settings for table */
            TableLimits tl = ((TableResult)result).getTableLimits();
            updateRateLimiters(((TableResult)result).getTableName(), tl);
        }
        /*
         * We may not have rate limiters yet because queries may
         * not have a tablename until after the first request.
         * So try to get rate limiters if we don't have them yet and
         * this is a QueryRequest.
         */
        if (rateLimiterMap != null && ctx.readLimiter == null) {
            ctx.readLimiter = getQueryRateLimiter(kvRequest, true);
        }
        if (rateLimiterMap != null && ctx.writeLimiter == null) {
            ctx.writeLimiter = getQueryRateLimiter(kvRequest, false);
        }
        return result;
    }

    /**
     * Handle rate limit from the Result.
     * This will consume actual units used by the request and sleep.
     */
    private CompletableFuture<Result> handlePostRateLimit(RequestContext ctx,
                                                          Result result) {
        final Request kvRequest = ctx.kvRequest;
        int postRateLimitDelayMs = consumeLimiterUnits(ctx.readLimiter,
                result.getReadUnitsInternal());
        postRateLimitDelayMs += consumeLimiterUnits(ctx.writeLimiter,
                result.getWriteUnitsInternal());

        return createDelayFuture(postRateLimitDelayMs)
            .thenApply(rateDelay -> {
                ctx.rateDelayedMs.addAndGet(rateDelay);
                result.setRateLimitDelayedMs(ctx.rateDelayedMs.get());

                /* copy retry stats to Result on successful operation */
                result.setRetryStats(kvRequest.getRetryStats());
                kvRequest.setRateLimitDelayedMs(ctx.rateDelayedMs.get());

                statsControl.observe(kvRequest,
                    Math.toIntExact(ctx.networkLatency),
                    ctx.reqSize, ctx.resSize);
                checkAuthRefreshList(kvRequest);
                return result;
            });
    }

    /*
     * Main error handling entry point.
     */
    private CompletableFuture<Result> handleError(RequestContext ctx,
                                                  Throwable err) {
        final Throwable actualCause =
            (err instanceof CompletionException && err.getCause() != null) ?
            err.getCause() : err;

        /* set exception on context */
        ctx.exception = actualCause;

        /* Get the appropriate error handler and delegate */
        ErrorHandler handler = findErrorHandler(actualCause.getClass());
        if (handler != null) {
            return handler.handle(ctx, actualCause);
        }

        /* Default throwable: retry with small delay */
        final String name = actualCause.getClass().getName();
        logInfo(logger, "Client execute Throwable, name: " +
                name + "message: " + actualCause.getMessage());
        return retryRequest(ctx, 10, actualCause);
    }

    /*
     * Initializes the error handlers map with specific exception types
     * and their corresponding handling strategies.
     * This method sets up a mapping between various exception classes
     * and the methods responsible for handling them,facilitating appropriate
     * error management and retry logic during request execution.
     */
    private void initErrorHandlers() {
        errorHandlers.put(AuthenticationException.class,
            this::handleAuthException);
        errorHandlers.put(InvalidAuthorizationException.class,
            this::handleInvalidAuthError);
        errorHandlers.put(SecurityInfoNotReadyException.class,
            this::handleSecurityNotReadyError);
        errorHandlers.put(RetryableException.class,
            this::handleRetryableError);
        errorHandlers.put(UnsupportedQueryVersionException.class,
            this::handleQueryVerError);
        errorHandlers.put(UnsupportedProtocolException.class,
            this::handleProtocolVerError);
        errorHandlers.put(RequestTimeoutException.class, this::failRequest);
        errorHandlers.put(NoSQLException.class, this::failRequest);
        errorHandlers.put(RuntimeException.class, this::failRequest);
        errorHandlers.put(IOException.class, this::handleIOError);
        errorHandlers.put(InterruptedException.class,
            this::handleInterruptedError);
        errorHandlers.put(ExecutionException.class, this::handleExecutionError);
        errorHandlers.put(TimeoutException.class, this::handleTimeoutError);
        /* Add any new error handlers here */
    }

    /*
     * Marks the request as failed and returns failed {@link CompletableFuture}.
     */
    private CompletableFuture<Result> failRequest(RequestContext ctx,
                                                  Throwable ex) {
        final String name = ex.getClass().getName();
        final String message = String.format("Client execute %s: %s",
            name, ex.getMessage());
        logFine(logger, message);
        ctx.kvRequest.setRateLimitDelayedMs(ctx.rateDelayedMs.get());
        statsControl.observeError(ctx.kvRequest);
        return CompletableFuture.failedFuture(ex);
    }

    /*
     * Schedules a retry for the request with the given delay.
     * Updates retry counters and statistics.
     */
    private CompletableFuture<Result> retryRequest(RequestContext ctx,
                                                   int delayMs, Throwable ex) {
        Request kvRequest = ctx.kvRequest;
        /* query and protocol exceptions are not errors, do not add them to
         * retry stats.
         */
        if (!(ex instanceof UnsupportedProtocolException
            || ex instanceof UnsupportedQueryVersionException)) {
            kvRequest.addRetryException(ex.getClass());
            kvRequest.incrementRetries();
            kvRequest.addRetryDelayMs(delayMs);
        }
        return scheduleRetry(ctx, delayMs);
    }

    /**
     * Looks up an error handler for the given class by traversing the
     * class hierarchy until a registered handler is found.
     *
     * @param clazz Exception class to resolve
     * @return A matching {@link ErrorHandler} or {@code null} if none found
     */
    private ErrorHandler findErrorHandler(Class<?> clazz) {
        while (clazz != null) {
            if (errorHandlers.containsKey(clazz)) {
                return errorHandlers.get(clazz);
            }
            clazz = clazz.getSuperclass();
        }
        return null;
    }

    /*
     * Error handler for {@link AuthenticationException}
     */
    private CompletableFuture<Result> handleAuthException(RequestContext ctx,
                                                          Throwable ex) {

        if (authProvider instanceof StoreAccessTokenProvider) {
            authProvider.flushCache();
            return retryRequest(ctx, 0, ex);
        } else {
            logInfo(logger, "Unexpected authentication exception: " + ex);
            return failRequest(ctx, new NoSQLException(
                "Unexpected exception: " + ex.getMessage(), ex));
        }
    }

    /*
     * Error handler for {@link InvalidAuthorizationException}
     */
    private CompletableFuture<Result> handleInvalidAuthError(RequestContext ctx,
                                                             Throwable ex) {
        Request kvRequest = ctx.kvRequest;
        if (kvRequest.getNumRetries() > 0) {
            return failRequest(ctx, ex);
        }
        authProvider.flushCache();
        logFine(logger,
            "Client retrying on InvalidAuthorizationException: "
            + ex.getMessage());
        return retryRequest(ctx, 0, ex);
    }

    /*
     * Error handler for {@link SecurityInfoNotReadyException}
     */
    private CompletableFuture<Result> handleSecurityNotReadyError(
        RequestContext ctx, Throwable ex) {

        Request kvRequest = ctx.kvRequest;
        int delayMs = SEC_ERROR_DELAY_MS;
        if (kvRequest.getNumRetries() > 10) {
            delayMs = DefaultRetryHandler.computeBackoffDelay(kvRequest, 0);
            if (delayMs <= 0) {
                return failRequest(ctx,
                    new RequestTimeoutException(ctx.timeoutMs,
                        ctx.requestClass + " timed out:" +
                        (ctx.requestId.isEmpty() ? "" :
                        " requestId=" + ctx.requestId) +
                        " nextRequestId=" + nextRequestId() +
                        (kvRequest.getRetryStats() != null ?
                        kvRequest.getRetryStats() : ""), ctx.exception));
            }
        }
        return retryRequest(ctx, delayMs, ex);
    }


    /*
     * Error handler for {@link RetryableException}
     */
    private CompletableFuture<Result> handleRetryableError(RequestContext ctx,
                                                           Throwable ex) {
        Request kvRequest = ctx.kvRequest;

        if (ex instanceof WriteThrottlingException && ctx.writeLimiter != null) {
            /* ensure we check write limits next retry */
            ctx.checkWriteUnits = true;
            /* set limiter to its limit, if not over already */
            if (ctx.writeLimiter.getCurrentRate() < 100.0) {
                ctx.writeLimiter.setCurrentRate(100.0);
            }
        }
        if (ex instanceof ReadThrottlingException && ctx.readLimiter != null) {
            /* ensure we check read limits next loop */
            ctx.checkReadUnits = true;
            /* set limiter to its limit, if not over already */
            if (ctx.readLimiter.getCurrentRate() < 100.0) {
                ctx.readLimiter.setCurrentRate(100.0);
            }
        }
        logFine(logger, "Retryable exception: " + ex.getMessage());
        /*
         * Handle automatic retries. If this does not throw an error,
         * then the delay (if any) will have been performed and the
         * request should be retried.
         *
         * If there have been too many retries this method will
         * throw the original exception.
         */
        int delayMs = handleRetry((RetryableException) ex, kvRequest);
        return retryRequest(ctx, delayMs, ex);
    }

    /*
     * Error handler for {@link UnsupportedQueryVersionException}
     */
    private CompletableFuture<Result> handleQueryVerError(RequestContext ctx,
                                                          Throwable ex) {
        if (decrementQueryVersion(ctx.queryVersionUsed)) {
            logFine(logger, "Got unsupported query version error " +
                "from server: decrementing query version to " +
                queryVersion + " and trying again.");
            return retryRequest(ctx, 0, ex);
        }
        return failRequest(ctx, ex);
    }

    /*
     * Error handler for {@link UnsupportedProtocolException}
     */
    private CompletableFuture<Result> handleProtocolVerError(RequestContext ctx,
                                                             Throwable ex) {
        if (decrementSerialVersion(ctx.serialVersionUsed)) {
            logFine(logger, "Got unsupported protocol error " +
                "from server: decrementing serial version to " +
                serialVersion + " and trying again.");
            return retryRequest(ctx, 0, ex);
        }
        return failRequest(ctx, ex);
    }

    /*
     * Error handler for {@link IOException}
     */
    private CompletableFuture<Result> handleIOError(RequestContext ctx,
                                                    Throwable ex) {
        Request kvRequest = ctx.kvRequest;
        String name = ex.getClass().getName();
        logFine(logger, "Client execution IOException, name: " +
            name + ", message: " + ex.getMessage());
        if (kvRequest.getNumRetries() > 10) {
            return failRequest(ctx, ex);
        }
        return retryRequest(ctx, 10, ex);
    }

    private CompletableFuture<Result> handleInterruptedError(RequestContext ctx,
                                                             Throwable ex) {
        logInfo(logger, "Interrupted: " + ex.getMessage());
        return failRequest(ctx,
            new NoSQLException("Request interrupted: " + ex.getMessage()));
    }

    private CompletableFuture<Result> handleExecutionError(RequestContext ctx,
                                                           Throwable ex) {
        /*
         * This can happen if a channel is bad in HttpClient.getChannel.
         * This happens if the channel is shut down by the server side
         * or the server (proxy) is restarted, etc. Treat it like
         * IOException above, but retry without waiting
         */
        String name = ex.getCause().getClass().getName();
        logFine(logger, "Client ExecutionException, name: " +
            name + ", message: " + ex.getMessage() + ", retrying");
        return retryRequest(ctx, 10, ex);
    }

    private CompletableFuture<Result> handleTimeoutError(RequestContext ctx,
                                                         Throwable ex) {
        logInfo(logger, "Timeout exception: " + ex);
        return failRequest(ctx,
            new RequestTimeoutException(
            ctx.timeoutMs,
            ctx.requestClass + " timed out:" +
            (ctx.requestId.isEmpty() ? "" : " requestId=" + ctx.requestId) +
            " nextRequestId=" + nextRequestId() +
            (ctx.kvRequest.getRetryStats() != null ?
                ctx.kvRequest.getRetryStats() : ""),
            ctx.exception));
    }

    /**
     * Helper method to create a CompletableFuture that completes after a delay.
     * This is used for non-blocking asynchronous delays for rate limiting.
     *
     * @param delayMs The delay in milliseconds.
     * @return A CompletableFuture that completes after the specified delay.
     */
    private CompletableFuture<Integer> createDelayFuture(int delayMs) {
        CompletableFuture<Integer> delayFuture = new CompletableFuture<>();
        if (delayMs > 0) {
            taskExecutor.schedule(() -> delayFuture.complete(delayMs), delayMs,
                TimeUnit.MILLISECONDS);
        } else {
            delayFuture.complete(delayMs); // Complete immediately if no delay
        }
        return delayFuture;
    }

    private CompletableFuture<Result>  scheduleRetry(RequestContext ctx,
                                                     int delayMs) {
        //TODO check for overall timeout before schedule
        CompletableFuture<Result> retryFuture = new CompletableFuture<>();
        taskExecutor.schedule(() -> {
            /* Increment request-id for retry */
            ctx.requestId = String.valueOf(ctx.nextIdSupplier.get());
            executeWithRetry(ctx)
            .whenComplete((res, e) -> {
                ctx.kvRequest.addRetryDelayMs(delayMs);
                if (e != null) {
                    retryFuture.completeExceptionally(e);
                } else {
                    retryFuture.complete(res);
                }
            });
        }, delayMs, TimeUnit.MILLISECONDS);
        return retryFuture;
    }
    /**
     * Calculate the timeout for the next iteration.
     * This is basically the given timeout minus the time
     * elapsed since the start of the request processing.
     * If this returns zero or negative, the request should be
     * aborted with a timeout exception.
     */
    private int getIterationTimeoutMs(long timeoutMs, long startNanos) {
        long diffNanos = System.nanoTime() - startNanos;
        return ((int)timeoutMs - Math.toIntExact(diffNanos / 1_000_000));
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
                                    long units) {

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
        return ((SimpleRateLimiter) rl).consumeExternally(units);
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
            logFine(logger, "removing rate limiting from table " + tableName);
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
        if (isLoggable(logger, Level.FINE)) {
            final String msg = String.format("Updated table '%s' to have " +
                "RUs=%.1f and WUs=%.1f per second", tableName, RUs, WUs);
            logFine(logger, msg);
        }

        return true;
    }


    /**
     * Determine if the request should be timed out.
     * Check if the request exceeds the timeout given.
     *
     * @param startNanos when the request starts
     * @param requestTimeoutMs the timeout of this request in ms
     * @param exception the last exception
     *
     * @return true if the request needs to be timed out.
     */
    boolean timeoutRequest(long startNanos,
                           long requestTimeoutMs,
                           Throwable exception) {
        return (getIterationTimeoutMs(requestTimeoutMs, startNanos) <= 0);
    }

    /**
     * Serializes the request payload, sent as http content
     *
     * @param content the buffer to contain the content
     *
     * @throws IOException
     */
    private void writeContent(ByteBuf content, RequestContext ctx)
        throws IOException {

        final Request kvRequest = ctx.kvRequest;
        final NettyByteOutputStream bos = new NettyByteOutputStream(content);
        ctx.serialVersionUsed = serialVersion;
        ctx.queryVersionUsed = queryVersion;

        SerializerFactory factory = chooseFactory(kvRequest);
        factory.writeSerialVersion(ctx.serialVersionUsed, bos);
        if (kvRequest instanceof QueryRequest ||
            kvRequest instanceof PrepareRequest) {
            kvRequest.createSerializer(factory).serialize(kvRequest,
                                                          ctx.serialVersionUsed,
                                                          ctx.queryVersionUsed,
                                                          bos);
        } else {
            kvRequest.createSerializer(factory).serialize(kvRequest,
                                                          ctx.serialVersionUsed,
                                                          bos);
        }
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
                                 RequestContext ctx) {

        if (!HttpResponseStatus.OK.equals(status)) {
            processNotOKResponse(status, content);

            /* TODO: Generate and handle bad status other than 400 */
            throw new IllegalStateException("Unexpected http response status:" +
                                            status);
        }

        setSessionCookie(headers);

        Result res = null;
        try (ByteInputStream bis = new NettyByteInputStream(content)) {
            res = processOKResponse(bis, ctx.kvRequest, ctx.serialVersionUsed,
                                    ctx.queryVersionUsed);
        }
        String sv = headers.get(SERVER_SERIAL_VERSION);
        if (sv != null) {
            try {
                res.setServerSerialVersion(Integer.parseInt(sv));
            } catch (Exception e) {
                /* ignore */
            }
        }
        return res;
    }

    /**
     * Process an OK response
     *
     * @return the result of processing the successful request
     *
     * @throws NoSQLException if the stream could not be read for some reason
     */
    Result processOKResponse(ByteInputStream in, Request kvRequest,
                             short serialVersionUsed, short queryVersionUsed) {
        try {
            SerializerFactory factory = chooseFactory(kvRequest);
            int code = factory.readErrorCode(in);
            /* note: this will always be zero in V4 */
            if (code == 0) {
                Result res;
                Serializer ser = kvRequest.createDeserializer(factory);
                if (kvRequest instanceof QueryRequest ||
                    kvRequest instanceof PrepareRequest) {
                    prepareResponseTestHook(kvRequest, in,
                                            serialVersionUsed,
                                            queryVersionUsed);
                    res = ser.deserialize(kvRequest,
                                          in,
                                          serialVersionUsed,
                                          queryVersionUsed);
                } else {
                    res = ser.deserialize(kvRequest,
                                          in,
                                          serialVersionUsed);
                }

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

    private void setSessionCookieValue(String pVal) {
        ConcurrentUtil.synchronizedCall(this.lock, () -> {
            sessionCookie = pVal;
        });
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
    private void backgroundUpdateLimiters(String tableName,
                                          String compartmentId) {
        ConcurrentUtil.synchronizedCall(this.lock, () -> {
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
        });
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
            logFine(logger, "Starting GetTableRequest for table '" +
                tableName + "'");
            res = (TableResult) ConcurrentUtil.awaitFuture(this.execute(gtr));
        } catch (Exception e) {
            logFine(logger, "GetTableRequest for table '" +
                tableName + "' returned exception: " + e.getMessage());
        }

        if (res == null) {
            /* table doesn't exist? other error? */
            logFine(logger, "GetTableRequest for table '" +
                tableName + "' returned null");
            AtomicLong then = tableLimitUpdateMap.get(tableName);
            if (then != null) {
                /* allow retry after 100ms */
                then.set(System.nanoTime() + 100_000_000L);
            }
            return;
        }

        logFine(logger, "GetTableRequest completed for table '" +
            tableName + "'");
        /* update/add rate limiters for table */
        if (updateRateLimiters(tableName, res.getTableLimits())) {
            logFine(logger, "background thread added limiters for table '" +
                tableName + "'");
        }
    }


    private int handleRetry(RetryableException re,
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
        return handler.delayTime(kvRequest, numRetries, re);
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
    private boolean decrementSerialVersion(short versionUsed) {
        return ConcurrentUtil.synchronizedCall(this.lock, () -> {
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
        });
    }

    /**
     * @hidden
     *
     * Try to decrement the query protocol version.
     * @return true: version was decremented
     *         false: already at lowest version number.
     */
    private boolean decrementQueryVersion(short versionUsed) {
        return ConcurrentUtil.synchronizedCall(this.lock, () -> {
        if (queryVersion != versionUsed) {
            return true;
        }

        if (queryVersion == QueryDriver.QUERY_V3) {
            return false;
        }

        --queryVersion;
        return true;
        });
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
    private void addRequestToRefreshList(Request request) {
        ConcurrentUtil.synchronizedCall(this.lock, () -> {
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
        });
    }

    /**
     * @hidden
     * for internal use
     */
    public void oneTimeMessage(String msg) {
        ConcurrentUtil.synchronizedCall(this.lock, () -> {
        if (oneTimeMessages.add(msg) == false) {
            return;
        }
        logWarning(logger, msg);
        });
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

    /*
     * Cloud service only.
     *
     * The request content needs to be signed for cross-region requests
     * under these conditions:
     * 1. a request is being made by a client that will become a cross-region
     * request such as add/drop replica
     * 2. a client table request such as add/drop index or alter table that
     * operates on a multi-region table. In this case the operation is
     * automatically applied remotely so it's implicitly a cross-region
     * operation
     * 3. internal use calls that use an OBO token to make the actual
     * cross region call from with the NoSQL cloud service. In this case
     * the OBO token is non-null in the request
     *
     * In cases (1) and (2) the signing is required so that the service can
     * acquire an OBO token for the operation. In case (3) the OBO token
     * that's been acquired by the service is used for the actual
     * cross region operation.
     */
    private boolean requireContentSigned(Request request) {
        /*
         * if this client is not using the cloud no signing is required
         */
        if (!authProvider.forCloud()) {
            return false;
        }

        /*
         * See comment above for the logic. TableRequest is always signed
         * because in the client it's not known if the operation is on a
         * multi-region table or not. This is a small bit of overhead and
         * is ignored if the table is not multi-region
         *
         * The Request.oboToken is not required by non Java SDKs, remove
         * request.getOboToken() != null if there is no Request.oboToken
         */
        return request instanceof AddReplicaRequest ||
               request instanceof DropReplicaRequest ||
               request instanceof TableRequest ||
               request.getOboToken() != null;
    }

    /*
     * Returns the request content bytes
     */
    private byte[] getBodyBytes(ByteBuf buffer) {
        if (buffer.hasArray()) {
            return buffer.array();
        }

        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return bytes;
    }

    /**
     * @hidden
     * For testing use
     */
    public void setDefaultNamespace(String ns) {
        config.setDefaultNamespace(ns);
    }

    public TopologyInfo getTopology() {
        return topology;
    }

    private int getTopoSeqNum() {
        return ConcurrentUtil.synchronizedCall(this.lock, () ->
            (topology == null ? -1 : topology.getSeqNum()));
    }

    private void setTopology(TopologyInfo topo) {

        ConcurrentUtil.synchronizedCall(this.lock, () -> {
        if (topo == null) {
            return;
        }

        if (topology == null || topology.getSeqNum() < topo.getSeqNum()) {
            topology = topo;
            trace("New topology: " + topo, 1);
        }
        });
    }

    /*
     * @hidden
     * Test hook for collecting prepare responses
     */
    private void prepareResponseTestHook(Request kvReq,
                                     ByteInputStream in,
                                     short serialVersion,
                                     short queryVersion) throws IOException {
        if (prepareFilename == null ||
            !(kvReq instanceof PrepareRequest) ||
            !(in instanceof NettyByteInputStream)) {
            return;
        }
        int offset = in.getOffset();
        try {
            PrepareRequest pReq = (PrepareRequest) kvReq;
            NettyByteInputStream nis = (NettyByteInputStream) in;
            ByteBuf buf = nis.buffer();
            int numBytes = buf.readableBytes();
            byte[] bytes = new byte[numBytes];
            for (int x = 0; x < numBytes; x++) {
                bytes[x] = in.readByte();
            }
            try (DataOutputStream dos = new DataOutputStream(
                    new FileOutputStream(prepareFilename))) {
                logFine(logger, "Serializing prepare response to " +
                    prepareFilename);
                dos.write(bytes, 0, numBytes);
            } catch (Exception e) {
                System.err.println("Error writing serialized " +
                        "prepared result: " + e);
            }
            /* write statement, etc to properties file */
            Properties props = new Properties();
            props.setProperty("statement", pReq.getStatement());
            props.setProperty("getplan",
                    String.valueOf(pReq.getQueryPlan()));
            props.setProperty("serialversion",
                    String.valueOf(serialVersion));
            props.setProperty("queryversion",
                    String.valueOf(queryVersion));
            String fName = prepareFilename + ".props";
            try (FileOutputStream fos = new FileOutputStream(fName)) {
                logFine(logger, "Writing property file " + fName);
                props.store(fos, "");
            } catch (Exception e) {
                System.err.println("Error writing serialized " +
                        "prepared result: " + e);
            }
        } catch (IOException e) {
            System.err.println("Error writing serialized " +
                    "prepared result: " + e);
        }
        in.setOffset(offset);
    }
}
