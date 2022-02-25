/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
import static oracle.nosql.driver.util.BinaryProtocol.V2;
import static oracle.nosql.driver.util.BinaryProtocol.V3;
import static oracle.nosql.driver.util.CheckNull.requireNonNull;
import static oracle.nosql.driver.util.LogUtil.isLoggable;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.LogUtil.logTrace;
import static oracle.nosql.driver.util.LogUtil.logWarning;
import static oracle.nosql.driver.util.HttpConstants.ACCEPT;
import static oracle.nosql.driver.util.HttpConstants.CONNECTION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_LENGTH;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.driver.util.HttpConstants.NOSQL_DATA_PATH;
import static oracle.nosql.driver.util.HttpConstants.REQUEST_ID_HEADER;
import static oracle.nosql.driver.util.HttpConstants.USER_AGENT;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.RateLimiter;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.RequestSizeLimitException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.RetryHandler;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.DurableRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.HttpConstants;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.RateLimiterMap;
import oracle.nosql.driver.util.SerializationUtil;

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

    /**
     * This may be configurable, but for now there is only one implementation
     */
    private final SerializerFactory factory = new BinarySerializerFactory();

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

    private short serialVersion = DEFAULT_SERIAL_VERSION;

    /* for one-time messages */
    private final HashSet<String> oneTimeMessages;

    /**
     * config for statistics
     */
    private StatsControlImpl statsControl;

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
        httpClient = new HttpClient(url.getHost(),
                                    url.getPort(),
                                    httpConfig.getNumThreads(),
                                    httpConfig.getConnectionPoolSize(),
                                    httpConfig.getPoolMaxPending(),
                                    httpConfig.getMaxContentLength(),
                                    httpConfig.getMaxChunkSize(),
                                    sslCtx,
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
     *
     * This method handles exceptions to distinguish between what can be retried
     * what what cannot, making sure that root cause exceptions are
     * kept. Examples:
     *  o can't connect (host, port, etc)
     *  o throttling exceptions
     *  o general networking issues, IOException
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

        /* clear any retry stats that may exist on this request object */
        kvRequest.setRetryStats(null);

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
                    new ResponseHandler(httpClient, logger, channel, requestId);

                buffer = channel.alloc().directBuffer();
                buffer.retain();

                /*
                 * we expressly check size limit below based on onprem versus
                 * cloud. Set the request to not check size limit inside
                 * writeContent().
                 */
                kvRequest.setCheckRequestSize(false);

                writeContent(buffer, kvRequest);

                /*
                 * If on-premise the authProvider will always be a
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

                authProvider.setRequiredHeaders(authString, kvRequest, headers);

                if (isLoggable(logger, Level.FINE)) {
                    logTrace(logger, "Request: " + requestClass);
                }
                networkLatency = System.currentTimeMillis();
                httpClient.runRequest(request, responseHandler, channel);

                boolean isTimeout =
                    responseHandler.await(thisIterationTimeoutMs);
                if (isTimeout) {
                    throw new TimeoutException("Request timed out after " +
                        timeoutMs + " milliseconds: requestId=" + requestId);
                }

                if (isLoggable(logger, Level.FINE)) {
                    logTrace(logger, "Response: " + requestClass + ", status " +
                             responseHandler.getStatus());
                }

                ByteBuf wireContent = responseHandler.getContent();
                Result res = processResponse(responseHandler.getStatus(),
                                       wireContent,
                                       kvRequest);
                int resSize = wireContent.readerIndex();
                networkLatency = System.currentTimeMillis() - networkLatency;

                if (serialVersion < 3) {
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
                if (decrementSerialVersion() == true) {
                    exception = upe;
                    logInfo(logger, "Got unsupported protocol error " +
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
                logFine(logger, "Client execute runtime exception: " +
                        e.getMessage());
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
            return rateLimiterMap.getReadLimiter(tableName);
        }
        return rateLimiterMap.getWriteLimiter(tableName);
    }

    /**
     * Comsume rate limiter units after successful operation.
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
         * 1) We don't know the actual number of units an op uses unitl
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
    void writeContent(ByteBuf content, Request kvRequest)
        throws IOException {

        final NettyByteOutputStream bos = new NettyByteOutputStream(content);
        bos.writeShort(serialVersion);
        kvRequest.createSerializer(factory).
            serialize(kvRequest,
                      serialVersion,
                      bos);
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
                                 ByteBuf content,
                                 Request kvRequest) {

        if (!HttpResponseStatus.OK.equals(status)) {
            processNotOKResponse(status, content);

            /* TODO: Generate and handle bad status other than 400 */
            throw new IllegalStateException("Unexpected http response status:" +
                                            status);
        }

        try (ByteInputStream bis = new NettyByteInputStream(content)) {
            return processOKResponse(bis, kvRequest);
        }
    }

    /**
     * Process an OK response
     *
     * @return the result of processing the successful request
     *
     * @throws IOException if the stream could not be read for some reason
     */
    Result processOKResponse(ByteInputStream in, Request kvRequest) {
        try {
            int code = in.readByte();
            if (code == 0) {
                Result res = kvRequest.createDeserializer(factory).
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
            throw handleResponseErrorCode(code, err);
        } catch (IOException e) {
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

    private void addCommonHeaders(HttpHeaders headers) {
        headers.set(CONTENT_TYPE, "application/octet-stream")
            .set(CONNECTION, "keep-alive")
            .set(ACCEPT, "application/octet-stream")
            .set(USER_AGENT, getUserAgent());
    }

    private static String getUserAgent() {
        return HttpConstants.userAgent;
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
    public boolean decrementSerialVersion() {
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
     * for internal use
     */
    public synchronized void oneTimeMessage(String msg) {
        if (oneTimeMessages.add(msg) == false) {
            return;
        }
        logWarning(logger, msg);
    }
}
