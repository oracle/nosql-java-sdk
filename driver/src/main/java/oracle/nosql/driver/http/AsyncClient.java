package oracle.nosql.driver.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import io.netty.util.IllegalReferenceCountException;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.InvalidAuthorizationException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.OperationNotSupportedException;
import oracle.nosql.driver.RequestSizeLimitException;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.UnsupportedProtocolException;
import oracle.nosql.driver.UnsupportedQueryVersionException;
import oracle.nosql.driver.httpclient.HttpResponse;
import oracle.nosql.driver.httpclient.ReactorHttpClient;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.DurableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.HttpConstants;
import oracle.nosql.driver.util.NettyByteInputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.MapValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetrySpec;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static oracle.nosql.driver.util.BinaryProtocol.*;
import static oracle.nosql.driver.util.CheckNull.requireNonNull;
import static oracle.nosql.driver.util.HttpConstants.*;

public class AsyncClient {

    private final Logger logger;
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


    private final ReactorHttpClient httpClient;
    private final AuthorizationProvider authProvider;
    private final boolean useSSL;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private Map<String, AtomicLong> tableLimitUpdateMap;

    /* update table limits once every 10 minutes */
    private static long LIMITER_REFRESH_NANOS = 600_000_000_000L;

    /*
     * amount of time between retries when security information
     * is unavailable
     */
    private static final int SEC_ERROR_DELAY_MS = 100;

    private AtomicInteger serialVersion = new AtomicInteger(DEFAULT_SERIAL_VERSION);

    /* separate version for query compatibility */
    private AtomicInteger queryVersion = new AtomicInteger(QueryDriver.QUERY_VERSION);

    /* for one-time messages */
    private final HashSet<String> oneTimeMessages;

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
    private final AtomicReference<String> sessionCookie = new AtomicReference<>("");
    /* note this must end with '=' */
    private final String SESSION_COOKIE_FIELD = "session=";

    /* for keeping track of SDKs usage */
    private final String userAgent;

    private final AtomicReference<TopologyInfo> topology = new AtomicReference<>();

    private final AtomicInteger maxRequestId = new AtomicInteger(1);


    public AsyncClient(NoSQLHandleConfig config, Logger logger) {
        this.config = config;
        this.logger = logger;
        this.url = config.getServiceURL();

        logger.fine("Driver service URL:" + url.toString());

        final String protocol = config.getServiceURL().getProtocol();
        if (!("http".equalsIgnoreCase(protocol) ||
                "https".equalsIgnoreCase(protocol))) {
            throw new IllegalArgumentException("Unknown protocol:" + protocol);
        }

        kvRequestURI = config.getServiceURL().toString() + NOSQL_DATA_PATH;
        host = config.getServiceURL().getHost();

        useSSL = "https".equalsIgnoreCase(protocol);

        /*
         * This builds an insecure context, usable for testing only
         */
        SslContext sslCtx;
        if (useSSL) {
            sslCtx = config.getSslContext();
            if (sslCtx == null) {
                throw new IllegalArgumentException("Unable to configure https: "
                    + "SslContext is missing from config");
            }
        } else {
            sslCtx = null;
        }

        httpClient = ReactorHttpClient
            .builder()
            .host(host)
            .port(url.getPort())
            .sslContext(sslCtx)
            .logger(logger)
            .build();

        authProvider = config.getAuthorizationProvider();
        if (authProvider == null) {
            throw new IllegalArgumentException(
                "Must configure AuthorizationProvider to use HttpClient");
        }

        String extensionUserAgent = config.getExtensionUserAgent();
        if (extensionUserAgent != null) {
            userAgent = HttpConstants.userAgent +
                " " + extensionUserAgent;
        } else {
            this.userAgent = HttpConstants.userAgent;
        }
        oneTimeMessages = new HashSet<>();
    }

    public void shutdown() {
        //logFine(logger, "Shutting down driver http client");
        //TODO
    }

    private void initAndValidateRequest(Request kvRequest) {
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

        // TODO
        //kvRequest.setRateLimitDelayedMs(0);

        /*
         * If the request doesn't set an explicit compartment, use
         * the config default if provided.
         */
        if (kvRequest.getCompartment() == null) {
            kvRequest.setCompartmentInternal(config.getDefaultCompartment());
        }
    }

    public Mono<Result> execute(Request kvRequest) {
        requireNonNull(kvRequest, "NoSQLHandle: request must be non-null");
        initAndValidateRequest(kvRequest);

        ClientRequest request = new ClientRequest(kvRequest,
                maxRequestId.getAndIncrement(),
                new AtomicInteger(serialVersion.get()),
                new AtomicInteger(queryVersion.get()));

        return executeWithTimeout(request);
    }

    private Mono<Result> executeWithTimeout(ClientRequest clientRequest) {
        return executeWithRetry(clientRequest)
        .timeout(Duration.ofMillis(clientRequest.kvRequest.getTimeoutInternal()));
    }

    private Mono<Result> getHttpMono(ClientRequest clientRequest) {
        final Request kvRequest = clientRequest.kvRequest;
        final String requestClass = kvRequest.getClass().getSimpleName();
        final String requestId = Long.toString(clientRequest.requestId.get());
        final AtomicInteger queryVersionUsed = clientRequest.queryVersionUsed;
        final AtomicInteger serialVersionUsed = clientRequest.serialVersionUsed;

        return Mono.defer(() -> {
            logger.fine(getLogMessage(requestId, requestClass,
                    "Inside execute core part"));

            /* kvRequest.isQueryRequest() returns true if kvRequest is a
             * non-internal QueryRequest
             */
            if (kvRequest.isQueryRequest()) {
                QueryRequest qreq = (QueryRequest)kvRequest;
                /* Set the topo seq num in the request, if it has not been set
                 * already */
                kvRequest.setTopoSeqNum(getTopoSeqNum());
                /*
                 * The following "if" may be true for advanced queries only. For
                 * such queries, the "if" will be true (i.e., the QueryRequest will
                 * be bound with a QueryDriver) if and only if this is not the 1st
                 * execute() call for this query. In this case we just return a new,
                 * empty QueryResult. Actual computation of a result batch will take
                 * place when the app calls getResults() on the QueryResult.
                 */
                if (qreq.hasDriver()) {
                    logger.fine("QueryRequest has QueryDriver");
                    return Mono.just(new QueryResult(qreq, false));
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
                    logger.fine("QueryRequest has no QueryDriver, but is prepared");
                    QueryDriver driver = new QueryDriver(qreq);
                    driver.setClient(this);
                    return Mono.just(new QueryResult(qreq, false));
                }
                logger.fine("QueryRequest has no QueryDriver and is not prepared");
                qreq.incBatchCounter();
            }

            //requestId.set(Long.toString(maxRequestId.getAndIncrement()));
            serialVersionUsed.set(serialVersion.get());
            queryVersionUsed.set(queryVersion.get());

            logger.fine(getLogMessage(requestId, requestClass,
                    "serialVersionUsed= " + serialVersionUsed));

            logger.fine(getLogMessage(requestId, requestClass,
                    "queryVersionUsed= " + queryVersionUsed));

            if (serialVersionUsed.get() < 3 && kvRequest instanceof DurableRequest) {
                if (((DurableRequest) kvRequest).getDurability() != null) {
                    oneTimeMessage("The requested feature is not " +
                            "supported " + "by the connected server: Durability");
                }
            }

            if (serialVersionUsed.get() < 3 && kvRequest instanceof TableRequest) {
                TableLimits limits = ((TableRequest) kvRequest).getTableLimits();
                if (limits != null && limits.getMode() ==
                        TableLimits.CapacityMode.ON_DEMAND) {
                    oneTimeMessage("The requested feature is not " +
                            "supported " + "by the connected server:" +
                            " on demand " + "capacity table");
                }
            }

            /*
             * we expressly check size limit below based on onprem versus
             * cloud. Set the request to not check size limit inside
             * writeContent().
             */
            kvRequest.setCheckRequestSize(false);

            return Mono.using(
                () -> { // Bytebuf resource acquisition
                    ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
                    buffer.retain();
                    return buffer;
                },
                (buffer) -> { // Bytebuf resource use
                    try {
                        serialVersionUsed.set(writeContent(buffer, kvRequest,
                                (short) queryVersionUsed.get()));
                    } catch (IOException e) {
                        return Mono.error(e);
                    }

                    /*
                     * If on-premises the authProvider will always be a
                     * StoreAccessTokenProvider. If so, check against
                     * configurable limit. Otherwise check against internal
                     * hardcoded cloud limit.
                     */
                    if (authProvider instanceof StoreAccessTokenProvider) {
                        if (buffer.readableBytes() > httpClient.getMaxContentLength()) {
                            return Mono.error(new RequestSizeLimitException(
                                "The request size of " + buffer.readableBytes() +
                                " exceeded the limit of " + httpClient.getMaxContentLength())
                            );
                        }
                    } else {
                        kvRequest.setCheckRequestSize(true);
                        try {
                            BinaryProtocol.checkRequestSizeLimit(
                                    kvRequest, buffer.readableBytes());
                        } catch (Throwable t) {
                            return Mono.error(t);
                        }
                    }

                    final Mono<HttpHeaders> requestHeader = getHeader(kvRequest,
                        buffer)
                        .map(headers -> headers.add(REQUEST_ID_HEADER, requestId))
                        .doOnNext(header -> {
                            logger.fine(getLogMessage(requestId, requestClass,
                            "Sending request to Server"));
                        });

                    // Submit http request to reactor netty
                    Mono<HttpResponse> responseMono = httpClient
                        .postRequest(kvRequestURI, requestHeader, buffer)
                        .doOnNext(response -> {
                            logger.fine(getLogMessage(requestId, requestClass,
                                    "Response: status=" + response.getStatus()));
                        }).doOnCancel(() -> {
                            logger.fine(getLogMessage(requestId, requestClass,
                                    "Http request is cancelled"));
                        });
                    return processResponse(responseMono, clientRequest)
                        .doOnNext(result ->  {
                            setTopology(result.getTopology());
                        });
                },
                (buffer) -> { // Bytebuf resource cleanup
                    logger.fine(getLogMessage(requestId, requestClass,
                        "Cleaning ByteBuf with refCount=" +
                            buffer.refCnt()));
                    buffer.release(buffer.refCnt());
                }
            );
        });
    }

    private Mono<Result> executeWithRetry(ClientRequest clientRequest) {
        final String requestClass = clientRequest.getClass().getSimpleName();
        final String requestId = clientRequest.requestIdStr;
        final AtomicInteger queryVersionUsed = clientRequest.queryVersionUsed;
        final AtomicInteger serialVersionUsed = clientRequest.serialVersionUsed;

        return getHttpMono(clientRequest)
                // Authentication and InvalidAuthorizationException-Retry 1 time
                .retryWhen(RetrySpec.max(1)
                        .filter(throwable -> throwable instanceof AuthenticationException
                                || throwable instanceof InvalidAuthorizationException)
                        .doBeforeRetry(retrySignal -> {
                            authProvider.flushCache();
                        })
                )
                // SecurityInfoNotReadyException-Retry 10 times with 100ms backoff
                .retryWhen(RetrySpec.backoff(10, Duration.ofMillis(SEC_ERROR_DELAY_MS))
                        .filter(throwable -> throwable instanceof SecurityInfoNotReadyException)
                )
                // RetryableException-Retry 10 times with 200ms backoff + jitter
                .retryWhen(Retry.backoff(10, Duration.ofMillis(200)).jitter(0.2)
                        .filter(throwable -> throwable instanceof RetryableException)
                )
                /* UnsupportedQueryVersionException- Retry 10 times. filter will
                limit max retries to queryVersion */
                .retryWhen(RetrySpec.max(10)
                        .filter(throwable -> throwable instanceof UnsupportedQueryVersionException &&
                                decrementQueryVersion((short) queryVersionUsed.get()))
                        .doBeforeRetry(retrySignal -> {
                            logRetries(requestId, requestClass,
                                    retrySignal.totalRetries() + 1,
                                    retrySignal.failure());
                        })
                )
                /* UnsupportedProtocolException- Retry 10 times. filter will
                limit max retries to serialVersion */
                .retryWhen(RetrySpec.max(10)
                        .filter(throwable -> throwable instanceof UnsupportedProtocolException &&
                                decrementSerialVersion((short) serialVersionUsed.get()))
                )
                // IOException - Retry 2 times with 10ms delay
                .retryWhen(RetrySpec.fixedDelay(2, Duration.ofMillis(10))
                        .filter(throwable -> throwable instanceof IOException)
                );
    }

    private short writeContent(ByteBuf content, Request kvRequest,
                               short queryVersion) throws IOException {
        final NettyByteOutputStream bos = new NettyByteOutputStream(content);
        final short versionUsed = (short) serialVersion.get();
        SerializerFactory factory = chooseFactory(kvRequest);
        factory.writeSerialVersion(versionUsed, bos);
        if (kvRequest instanceof QueryRequest ||
                kvRequest instanceof PrepareRequest) {
            kvRequest.createSerializer(factory).serialize(kvRequest,
                versionUsed,
                queryVersion,
                bos);
        } else {
            kvRequest.createSerializer(factory).serialize(kvRequest,
                versionUsed,
                bos);
        }
        return versionUsed;
    }

    private SerializerFactory chooseFactory(Request rq) {
        if (serialVersion.get() == 4) {
            return v4factory;
        } else {
            return v3factory; /* works for v2 also */
        }
    }

    final Result processResponse(HttpResponseStatus status,
                                 HttpHeaders responseHeaders,
                                 ByteBuf responseBody,
                                 Request kvRequest,
                                 short serialVersionUsed,
                                 short queryVersionUsed) {
        if (!HttpResponseStatus.OK.equals(status)) {
            processNotOKResponse(status, responseBody);

            /* TODO: Generate and handle bad status other than 400 */
            throw new IllegalStateException("Unexpected http response status:" +
                status);
        }

        setSessionCookie(responseHeaders);

        try (ByteInputStream bis = new NettyByteInputStream(responseBody)) {
            return processOKResponse(bis, kvRequest, serialVersionUsed,
                queryVersionUsed);
        }
    }

    private Mono<Result> processResponse(Mono<HttpResponse> responseMono,
                                         ClientRequest clientRequest) {
        final String requestClass = clientRequest.requestClass;
        final String requestId = clientRequest.requestIdStr;
        final Request kvRequest = clientRequest.kvRequest;
        final AtomicInteger serialVersionUsed = clientRequest.serialVersionUsed;
        final AtomicInteger queryVersionUsed = clientRequest.queryVersionUsed;

        return responseMono.flatMap(httpResponse -> {
            HttpHeaders responseHeaders = httpResponse.getHeaders();
            HttpResponseStatus responseStatus = httpResponse.getStatus();
            Mono<ByteBuf> body = httpResponse.getBody();
            Mono<Result> resultMono = body.handle((content, sink) -> {
                logger.fine(getLogMessage(requestId,
                        requestClass, "processing response"));
                try {
                    Result res = processResponse(responseStatus,
                            responseHeaders,
                            content,
                            kvRequest,
                            (short) serialVersionUsed.get(),
                            (short) queryVersionUsed.get());
                    sink.next(res);

                    //TODO what to do for below
                        /*
                        rateDelayedMs += getRateDelayedFromHeader(
                            responseHandler.getHeaders());
                        int resSize = wireContent.readerIndex();
                        long networkLatency =
                            (System.nanoTime() - latencyNanos) / 1_000_000;
                        setTopology(res.getTopology());
                        */

                    // TODO is this required
                    /*if (serialVersionUsed < 3) {
                     *//* so we can emit a one-time message if the app *//*
                     *//* tries to access modificationTime *//*
                        if (res instanceof GetResult) {
                            ((GetResult)res).setClient(this);
                        } else if (res instanceof WriteResult) {
                            ((WriteResult)res).setClient(this);
                        }
                        }*/

                    //TODO is below required
                        /*if (res instanceof QueryResult && kvRequest.isQueryRequest()) {
                            QueryRequest qreq = (QueryRequest)kvRequest;
                            qreq.addQueryTraces(((QueryResult)res).getQueryTraces());
                        }*/
                    //return res;
                } catch (IllegalReferenceCountException e) {
                    logger.fine(getLogMessage(requestId,
                            requestClass,
                            "Illegal refCount, request might be " +
                                    "cancelled")
                    );
                    sink.complete();
                } catch (Throwable t) {
                    sink.error(t);
                }
            });
            return resultMono;
        });
    }

    /**
     * Process an OK response
     *
     * @return the result of processing the successful request
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
                        (WriteMultipleRequest) kvRequest);
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
     * @param status  the http response code it must not be OK
     * @param payload the payload representing the failure response
     */
    private void processNotOKResponse(HttpResponseStatus status,
                                      ByteBuf payload) {
        if (HttpResponseStatus.BAD_REQUEST.equals(status)) {
            int len = payload.readableBytes();
            String errMsg = (len > 0) ?
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
        if (v == null || !v.startsWith(SESSION_COOKIE_FIELD)) {
            return;
        }
        int semi = v.indexOf(";");
        v = (semi < 0) ? v : v.substring(0, semi);
        sessionCookie.set(v);
        logger.fine("Set session cookie to \"" + sessionCookie + "\"");
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

    public synchronized void oneTimeMessage(String msg) {
        if (!oneTimeMessages.add(msg)) {
            return;
        }
        logger.warning(msg);
    }

    private String getUserAgent() {
        return userAgent;
    }

    private String getSerdeVersion(Request rq) {
        return chooseFactory(rq).getSerdeVersionString();
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
                !msg.contains(",") ||
                msg.contains("[")) {
            return handleResponseErrorCode(code, msg);
        }
        throw new OperationNotSupportedException("WriteMultiple requests " +
                "using multiple tables are not supported by the " +
                "version of the connected server.");
    }

    /**
     * Map a specific error status to a specific exception.
     */
    private RuntimeException handleResponseErrorCode(int code, String msg) {
        throw BinaryProtocol.mapException(code, msg);
    }

    private String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    Flux<QueryResult> executeQuery(QueryRequest queryRequest) {
        requireNonNull(queryRequest, "NoSQLHandle: request must be non-null");
        logger.log(Level.INFO, "sending query request");
        initAndValidateRequest(queryRequest);

        // Use Flux.using to close the queryRequest once done using it.
        return Flux.using(
            () -> { // resource acquisition
                return queryRequest;
            },
            (qRequest) -> {  //Using the resource
                /* If queryRequest is already not prepared, send prepare request to
                 * server and set the PrepareResult on queryRequest. This is done to
                 * check if query is simple or complex. Complex queries uses query
                 * driver and PlanIter which contains blocking code and can't be called
                 * from netty nio threads and need to be process on BoundedElastic
                 * threads.
                 */
                Mono<QueryRequest> requestMono = Mono.just(qRequest)
                    .filter(qr -> !qr.isPrepared())
                    .flatMap(qr -> {
                        PrepareRequest prepareRequest = new PrepareRequest();
                        prepareRequest.setStatement(qr.getStatement())
                                .setCompartment(qr.getCompartment())
                                .setTableName(qr.getTableName())
                                .setNamespace(qr.getNamespace());
                        return execute(prepareRequest).cast(PrepareResult.class)
                                .doOnNext(qr::setPreparedStatement)
                                .then(Mono.just(qr));
                    })
                    .defaultIfEmpty(qRequest);

                /*
                 * Below code executes the query which return Mono<QueryResult> and
                 * recursively expands to produce Flux<QueryResult>. For complex
                 * quires QueryDriver is used to get the results, which is blocking,
                 * because of this for queries which have query driver publish the
                 * QueryResult on BoundedElastic threads. For simple queries request
                 * always goes to server without the need of query driver blocking
                 * part, so no need to publish on BoundedElastic for simple queries.
                 *
                 * This async code is same as
                 * below sync code.
                 *
                 *   do {
                 *     qureyResult = execute(queryRequest)
                 *   } while(qureyResult.getContinuationKey != null)
                 */
                Flux<QueryResult> recurseFlux = requestMono.flatMapMany(qr -> {
                    Mono<QueryResult> baseMono;
                    if (!qr.isSimpleQuery()) {
                        baseMono = execute(qr)
                                .cast(QueryResult.class)
                                .publishOn(Schedulers.boundedElastic())
                                .flatMap(result -> {
                                    /* compute the result. This will call
                                     * queryDriver and PlanIter to get results
                                     */
                                    result.getContinuationKey();
                                    return Mono.just(result);
                                });
                    } else {
                        baseMono = execute(qr).cast(QueryResult.class);
                    }
                    return baseMono.expand(result -> result.getContinuationKey() == null ?
                            Mono.empty() : baseMono);
                });
                return recurseFlux;
            },
            (qRequest) -> { // Resource cleanup
                /* TODO Who owns the QueryRequest? Consider both sync and
                    async case
                */

                //qRequest.close();
            }
        );
    }

    private Mono<HttpHeaders> getHeader(Request kvRequest, ByteBuf buffer) {
        /*
         * boolean that indicates whether content must be signed. Cross
         * region operations must include content when signing. See comment
         * on the method
         */
        final boolean signContent = requireContentSigned(kvRequest);

        //TODO check below line is having any blocking call
        return Mono.from(authProvider.getAuthorizationStringAsync(kvRequest))
            .handle((String authString, SynchronousSink<String> sink) -> {
                try {
                    authProvider.validateAuthString(authString);
                    sink.next(authString);
                } catch (Exception e) {
                    sink.error(e);
                }
            }).flatMap(authString -> {
                HttpHeaders headers = new DefaultHttpHeaders();
                headers.set(CONTENT_TYPE, "application/octet-stream")
                        .set(CONNECTION, KEEP_ALIVE)
                        .set(ACCEPT, "application/octet-stream")
                        .set(USER_AGENT, getUserAgent())
                        .set(HttpHeaderNames.HOST, host)
                        .setInt(CONTENT_LENGTH, buffer.readableBytes());

                if (sessionCookie.get() != null) {
                    headers.add(COOKIE, sessionCookie);
                }
                String serdeVersion = getSerdeVersion(kvRequest);
                if (serdeVersion != null) {
                    headers.add("x-nosql-serde-version", serdeVersion);
                }

                String namespace = kvRequest.getNamespace();
                if (namespace == null) {
                    namespace = config.getDefaultNamespace();
                }
                if (namespace != null) {
                    headers.add(REQUEST_NAMESPACE_HEADER, namespace);
                }
                byte[] content = signContent ? getBodyBytes(buffer) : null;
                //TODO check below line is having any blocking call
                return Mono.from(authProvider.setRequiredHeadersAsync(authString,
                        kvRequest,
                        headers, content)).then(Mono.just(headers));
            });
    }

    // TODO check whether this is equivalent to synchronized version
    private int getTopoSeqNum() {
        TopologyInfo topo = topology.get();
        return (topo == null ? -1 : topology.get().getSeqNum());
    }

    // TODO check whether this is equivalent to synchronized version
    private void setTopology(TopologyInfo topo) {
        if (topo == null) {
            return;
        }
        topology.accumulateAndGet(topo, (curTopo, newTopo) -> {
            if (curTopo == null || curTopo.getSeqNum() < newTopo.getSeqNum()) {
                logger.fine("New topology: " + newTopo);
                return newTopo;
            }
            return curTopo;
        });

    }

    /**
     * @hidden
     *
     * Try to decrement the query protocol version.
     * @return true: version was decremented
     *         false: already at lowest version number which is V3.
     */
    private boolean decrementQueryVersion(short versionUsed) {
        //TODO check whether V3 is lowest version supported
        if (versionUsed == QueryDriver.QUERY_V3) {
            return false;
        }
        queryVersion.compareAndSet(versionUsed, versionUsed-1);
        return true;
    }

    private boolean decrementSerialVersion(short versionUsed) {
        //TODO check whether V2 is lowest version supported
        if (versionUsed == V2) {
            return false;
        }
        serialVersion.compareAndSet(versionUsed, versionUsed-1);
        return true;
    }
    private void logRetries(String requestId,
                            String requeatClass,
                            long numRetries,
                            Throwable exception) {
        logger.fine(getLogMessage(requestId, requeatClass,
            "Doing retry: " + numRetries +
                (exception != null ? ", exception: " + exception : "")));
    }

    private String getLogMessage(String requestId, String request, String msg) {
        return "[Request " + requestId + "] " + request + "- " + msg;
    }

    public TopologyInfo getTopology() {
        return topology.get();
    }

    short getSerialVersion() {
        return (short) serialVersion.get();
    }

    /**
     * @hidden
     * For testing use
     */
    public void setDefaultNamespace(String ns) {
        config.setDefaultNamespace(ns);
    }

    /*
     * Helper class which contains all the information for async flow
     */
    private static class ClientRequest {
        private final Request kvRequest;
        private final String requestClass;
        private final AtomicInteger requestId;
        private final String requestIdStr;
        private final AtomicInteger serialVersionUsed;
        private final AtomicInteger queryVersionUsed;

        public ClientRequest(Request kvRequest,
                             int requestId,
                             AtomicInteger serialVersionUsed,
                             AtomicInteger queryVersionUsed) {
            this.kvRequest = kvRequest;
            this.requestClass = kvRequest.getClass().getSimpleName();
            this.requestId = new AtomicInteger(requestId);
            this.requestIdStr = Long.toString(requestId);
            this.serialVersionUsed = serialVersionUsed;
            this.queryVersionUsed = queryVersionUsed;
        }
    }
}
