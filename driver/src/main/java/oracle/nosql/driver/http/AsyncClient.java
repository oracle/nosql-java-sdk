package oracle.nosql.driver.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import oracle.nosql.driver.*;
import oracle.nosql.driver.httpclient.HttpResponse;
import oracle.nosql.driver.httpclient.ReactorHttpClient;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.ops.serde.BinarySerializerFactory;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.ops.serde.nson.NsonSerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.util.*;
import oracle.nosql.driver.values.MapValue;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

    Logger logger = Logger.getLogger(getClass().getName());
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


    private volatile short serialVersion = DEFAULT_SERIAL_VERSION;

    /* separate version for query compatibility */
    private volatile short queryVersion = QueryDriver.QUERY_VERSION;

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
    private volatile String sessionCookie;
    /* note this must end with '=' */
    private final String SESSION_COOKIE_FIELD = "session=";

    /* for keeping track of SDKs usage */
    private String userAgent;

    private volatile TopologyInfo topology;

    private final AtomicInteger maxRequestId = new AtomicInteger(1);


    public AsyncClient(NoSQLHandleConfig config) {
        this.config = config;
        this.url = config.getServiceURL();

        logger.fine( "Driver service URL:" + url.toString());

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
                throw new IllegalArgumentException(
                        "Unable to configure https: " +
                                "SslContext is missing from config");
            }
        } else {
            sslCtx = null;
        }

        httpClient = ReactorHttpClient.builder().host(host).port(url.getPort()).sslContext(sslCtx).build();

        authProvider = config.getAuthorizationProvider();
        if (authProvider == null) {
            throw new IllegalArgumentException(
                    "Must configure AuthorizationProvider to use HttpClient");
        }

        String extensionUserAgent = config.getExtensionUserAgent();
        if (extensionUserAgent != null) {
            userAgent = HttpConstants.userAgent +
                    " " +
                    extensionUserAgent;
        } else {
            this.userAgent = HttpConstants.userAgent;
        }

        oneTimeMessages = new HashSet<String>();
    }

    public void shutdown() {
        //logFine(logger, "Shutting down driver http client");
        //TODO
    }

    public Mono<Result> execute(Request kvRequest) {
        requireNonNull(kvRequest, "NoSQLHandle: request must be non-null");

        logger.log(Level.INFO, "sending execute request");
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

        return Mono.defer(() -> {
            ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();

            return Mono.defer(() -> {
                        logger.info("Inside execute core part");
                        buffer.retain();
                        if (serialVersion < 3 && kvRequest instanceof DurableRequest) {
                            if (((DurableRequest) kvRequest).getDurability() != null) {
                                oneTimeMessage("The requested feature is not supported " +
                                        "by the connected server: Durability");
                            }
                        }

                        if (serialVersion < 3 && kvRequest instanceof TableRequest) {
                            TableLimits limits = ((TableRequest) kvRequest).getTableLimits();
                            if (limits != null &&
                                    limits.getMode() == TableLimits.CapacityMode.ON_DEMAND) {
                                oneTimeMessage("The requested feature is not supported " +
                                        "by the connected server: on demand " +
                                        "capacity table");
                            }
                        }

                        short serialVersionUsed = serialVersion;
                        short queryVersionUsed = queryVersion;

                        logger.fine("serialVersionUsed= " + serialVersionUsed);
                        logger.fine("queryVersionUsed= " + queryVersionUsed);


                        /*
                         * we expressly check size limit below based on onprem versus
                         * cloud. Set the request to not check size limit inside
                         * writeContent().
                         */
                        kvRequest.setCheckRequestSize(false);

                        try {
                            buffer.clear();
                            serialVersionUsed = writeContent(buffer, kvRequest,
                                    queryVersionUsed);
                        } catch (IOException e) {
                            //TODO what to do
                            throw new RuntimeException(e);
                        }

                        /*
                         * If on-premises the authProvider will always be a
                         * StoreAccessTokenProvider. If so, check against
                         * configurable limit. Otherwise check against internal
                         * hardcoded cloud limit.
                         */
                        //TODO HANDLE THIS

                        HttpHeaders requestHeader = getHeader(kvRequest, buffer);


                        short finalSerialVersionUsed = serialVersionUsed;

                        // Submit http request to reactor netty
                        Mono<HttpResponse> responseMono = httpClient.postRequest(kvRequestURI, requestHeader, buffer);
                        return responseMono.flatMap(httpResponse -> {
                            HttpHeaders responseHeaders = httpResponse.getHeaders();
                            HttpResponseStatus responseStatus = httpResponse.getStatus();
                            Mono<ByteBuf> body = httpResponse.getBody();
                            return body.map(content -> {
                                Result res = processResponse(responseStatus, responseHeaders, content, kvRequest, finalSerialVersionUsed, queryVersionUsed);
                                setTopology(res.getTopology());
                                return res;
                            });
                        });
                    }).doOnError(error -> logger.info("Error occurred for " +
                            "request " + error))
                    .retryWhen(
                            RetrySpec.max(1)
                                    .filter(throwable -> throwable instanceof UnsupportedQueryVersionException &&
                                            decrementQueryVersion(queryVersion))
                    ).retryWhen(RetrySpec.max(1)
                            .filter(throwable -> throwable instanceof UnsupportedProtocolException &&
                                    decrementSerialVersion(serialVersion))
                    ).retryWhen(RetrySpec.max(1)
                            .filter(throwable -> throwable instanceof AuthenticationException)
                    ).retryWhen(RetrySpec.max(1)
                            .filter(throwable -> throwable instanceof InvalidAuthorizationException)
                    ).retryWhen(RetrySpec.max(10)
                            .filter(throwable -> throwable instanceof SecurityInfoNotReadyException)
                    ).retryWhen(Retry.backoff(2, Duration.ofMillis(1000))
                            .filter(throwable -> throwable instanceof RetryableException)
                    ).doFinally(signalType -> {
                                logger.fine("buffer refCount is " + buffer.refCnt());
                                if(buffer.refCnt() > 0) {
                                    buffer.release(buffer.refCnt());
                                }
                            }
                    );
        }).timeout(Duration.ofMillis(50000));
    }

    private short writeContent(ByteBuf content, Request kvRequest,
                               short queryVersion)
            throws IOException {

        final NettyByteOutputStream bos = new NettyByteOutputStream(content);
        final short versionUsed = serialVersion;
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
        if (serialVersion == 4) {
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
        logger.info("processing response for request " + responseHeaders.get(REQUEST_ID_HEADER));
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

                /*if (kvRequest.isQueryRequest()) {
                    QueryRequest qreq = (QueryRequest)kvRequest;
                    if (!qreq.isSimpleQuery()) {
                        qreq.getDriver().setClient(this);
                    }
                }*/
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
        if (v == null || v.startsWith(SESSION_COOKIE_FIELD) == false) {
            return;
        }
        int semi = v.indexOf(";");
        if (semi < 0) {
            setSessionCookieValue(v);
        } else {
            setSessionCookieValue(v.substring(0, semi));
        }
        /*if (isLoggable(logger, Level.TRACE)) {
            logTrace(logger, "Set session cookie to \"" + sessionCookie + "\"");
        }*/
    }

    private synchronized void setSessionCookieValue(String pVal) {
        sessionCookie = pVal;
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
        //logWarning(logger, msg);
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
        RuntimeException exc = BinaryProtocol.mapException(code, msg);
        throw exc;
    }

    private String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    public Mono<TableResult> doTableRequest(TableRequest request) {
        GetTableRequest getTableRequest = new GetTableRequest();

        return execute(request).cast(TableResult.class)
                .flatMapMany(tableResult -> {
                    getTableRequest
                            .setTableName(tableResult.getTableName())
                            .setOperationId(tableResult.getOperationId())
                            .setCompartment(tableResult.getCompartmentId());
                    return Flux.interval(Duration.ofSeconds(5))
                            .take(5)
                            .flatMap(i -> execute(getTableRequest).cast(TableResult.class))
                            .filter(res -> res.getTableState() == TableResult.State.ACTIVE)
                            .next();
                })
                .next();
       /* return doTableRequestInternal(request)
                .map(result -> {
                    getTableRequest.setTableName(result.getTableName())
                            .setOperationId(result.getOperationId())
                            .setCompartment(result.getCompartmentId());
                    return result;
                })
                .flatMap(result ->
                        doTableRequestInternal(getTableRequest)
                                .repeatWhen(repeat -> repeat.delayElements(Duration.ofMillis(1000)).take(5)) // Retry up to 5 times
                                .skipUntil(r -> r.getTableState() == TableResult.State.ACTIVE)
                                .switchIfEmpty(Mono.error(new RequestTimeoutException("timeout")))
                                .subscribeOn(Schedulers.boundedElastic())
                                .single() // Take the last emitted item
                );*/
    }

    Flux<Result> executeQuery(Request kvRequest) {
        requireNonNull(kvRequest, "NoSQLHandle: request must be non-null");

        logger.log(Level.INFO, "sending query request");
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

        int timeoutMs = kvRequest.getTimeoutInternal();
        Throwable exception = null;

        QueryRequest queryRequest = (QueryRequest) kvRequest;

        return Flux.<Result>create(emitter -> {
            Flux<Mono<Result>> innerPublisher = Flux.generate(
                    () -> queryRequest,
                    (state, sink) -> {
                        logger.info("Generate called ");
                        if (!state.isPrepared() || !state.isDone()) {
                            Mono<Result> nextPage = execute(state).doOnNext(result -> {
                                QueryResult result1 = (QueryResult) result;
                                state.setContKey(result1.getContinuationKey());
                            });
                            sink.next(nextPage);
                        } else {
                            sink.complete();
                        }
                        return state;
                    }
            );
            innerPublisher.doOnCancel(() -> logger.info("InnerPublisher " +
                    "cancelled"));
            AtomicReference<Subscription> innerSubscription = new AtomicReference<>();

            emitter.onRequest(n -> {
                logger.info(n + " request recived");
                if (innerSubscription.get() == null) {
                    innerPublisher.subscribe(new BaseSubscriber<Mono<Result>>() {
                        @Override
                        protected void hookOnSubscribe(Subscription subscription) {
                            innerSubscription.set(subscription);
                        }

                        @Override
                        protected void hookOnNext(Mono<Result> value) {
                            value.subscribe(emitter::next, emitter::error);
                        }

                        @Override
                        protected void hookOnError(Throwable throwable) {
                            emitter.error(throwable);
                        }

                        @Override
                        protected void hookOnComplete() {
                            emitter.complete();
                        }
                    });
                }
                innerSubscription.get().request(n);
            });
            emitter.onCancel(() -> logger.info("cancelling create flux"));
            emitter.onDispose(() -> innerSubscription.get().cancel());

        }).limitRate(1);
        //return generate.concatMap(resultMono -> resultMono);
    }

    private HttpHeaders getHeader(Request kvRequest, ByteBuf buffer) {
        /*
         * boolean that indicates whether content must be signed. Cross
         * region operations must include content when signing. See comment
         * on the method
         */
        final boolean signContent = requireContentSigned(kvRequest);

        final String authString = authProvider.getAuthorizationString(kvRequest);
        authProvider.validateAuthString(authString);

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.set(CONTENT_TYPE, "application/octet-stream")
                .set(CONNECTION, KEEP_ALIVE)
                .set(ACCEPT, "application/octet-stream")
                .set(USER_AGENT, getUserAgent())
                .set(HttpHeaderNames.HOST, host)
                .add(REQUEST_ID_HEADER, Long.toString(maxRequestId.getAndIncrement()))
                .setInt(CONTENT_LENGTH, buffer.readableBytes());

        if (sessionCookie != null) {
            headers.add(COOKIE, sessionCookie);
        }
        String serdeVersion = getSerdeVersion(kvRequest);
        if (serdeVersion != null) {
            headers.add("x-nosql-serde-version", serdeVersion);
        }

        if (kvRequest.getCompartment() == null) {
            kvRequest.setCompartmentInternal(config.getDefaultCompartment());
        }

        byte[] content = signContent ? getBodyBytes(buffer) : null;
        authProvider.setRequiredHeaders(authString, kvRequest, headers, content);

        String namespace = kvRequest.getNamespace();
        if (namespace == null) {
            namespace = config.getDefaultNamespace();
        }
        if (namespace != null) {
            headers.add(REQUEST_NAMESPACE_HEADER, namespace);
        }
        return headers;
    }

    private synchronized int getTopoSeqNum() {
        return (topology == null ? -1 : topology.getSeqNum());
    }

    private synchronized void setTopology(TopologyInfo topo) {
        if (topo == null) {
            return;
        }

        if (topology == null || topology.getSeqNum() < topo.getSeqNum()) {
            topology = topo;
            logger.fine("New topology: " + topo);
        }
    }

    private synchronized boolean decrementQueryVersion(short versionUsed) {
        if (queryVersion != versionUsed) {
            return true;
        }
        if (queryVersion == QueryDriver.QUERY_V4) {
            queryVersion = QueryDriver.QUERY_V3;
            return true;
        }
        return false;
    }

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
}
