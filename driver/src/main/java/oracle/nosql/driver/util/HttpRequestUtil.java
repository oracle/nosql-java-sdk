/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.driver.util.LogUtil.logFine;
import static oracle.nosql.driver.util.LogUtil.logInfo;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_LENGTH;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.httpclient.ResponseHandler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Utility to issue HTTP request using {@link HttpClient}.
 */
public class HttpRequestUtil {
    private static final Charset utf8 = Charset.forName("UTF-8");
    private static final int DEFAULT_DELAY_MS = 1000;

    /**
     * Issue HTTP GET request using given HTTP client with retries and general
     * error handling. This method is re-entrant.
     *
     * It retries upon seeing following exceptions and response codes:
     * <ul>
     * <li>IOException</li>
     * <li>HTTP response with status code larger than 500</li>
     * <li>Other throwable excluding RuntimeException, InterruptedException,
     * ExecutionException and TimeoutException</li>
     * </ul>
     *
     * @param httpClient a HTTP client
     *
     * @param uri the request URI
     *
     * @param headers HTTP headers of this request
     *
     * @param timeoutMs request timeout in milliseconds
     *
     * @param logger logger
     *
     * @return HTTP response, a object encapsulate status code and response
     */
    public static HttpResponse doGetRequest(HttpClient httpClient,
                                            String uri,
                                            HttpHeaders headers,
                                            int timeoutMs,
                                            Logger logger) {

        return doRequest(httpClient, uri, headers, GET,
                         null /* no payload */, timeoutMs, logger);
    }

    /**
     * Issue HTTP POST request using given HTTP client with retries and general
     * error handling. This method is re-entrant.
     *
     * It retries upon seeing following exceptions and response codes:
     * <ul>
     * <li>IOException</li>
     * <li>HTTP response with status code larger than 500</li>
     * <li>Other throwable excluding RuntimeException, InterruptedException,
     * ExecutionException and TimeoutException</li>
     * </ul>
     *
     * @param httpClient a HTTP client
     *
     * @param uri the request URI
     *
     * @param headers HTTP headers of this request
     *
     * @param payload payload in byte array
     *
     * @param timeoutMs request timeout in milliseconds
     *
     * @param logger logger
     *
     * @return HTTP response, a object encapsulate status code and response
     */
    public static HttpResponse doPostRequest(HttpClient httpClient,
                                             String uri,
                                             HttpHeaders headers,
                                             byte[] payload,
                                             int timeoutMs,
                                             Logger logger) {

        return doRequest(httpClient, uri, headers, POST,
                         payload, timeoutMs, logger);
    }

    /**
     * Issue HTTP PUT request using given HTTP client with retries and general
     * error handling. This method is re-entrant.
     *
     * It retries upon seeing following exceptions and response codes:
     * <ul>
     * <li>IOException</li>
     * <li>HTTP response with status code larger than 500</li>
     * <li>Other throwable excluding RuntimeException, InterruptedException,
     * ExecutionException and TimeoutException</li>
     * </ul>
     *
     * @param httpClient a HTTP client
     *
     * @param uri the request URI
     *
     * @param headers HTTP headers of this request
     *
     * @param payload payload in byte array
     *
     * @param timeoutMs request timeout in milliseconds
     *
     * @param logger logger
     *
     * @return HTTP response, a object encapsulate status code and response
     */
    public static HttpResponse doPutRequest(HttpClient httpClient,
                                            String uri,
                                            HttpHeaders headers,
                                            byte[] payload,
                                            int timeoutMs,
                                            Logger logger) {

        return doRequest(httpClient, uri, headers, PUT,
                         payload, timeoutMs, logger);
    }

    /**
     * Issue HTTP DELETE request using given HTTP client with retries and
     * general error handling. This method is re-entrant.
     *
     * It retries upon seeing following exceptions and response codes:
     * <ul>
     * <li>IOException</li>
     * <li>HTTP response with status code larger than 500</li>
     * <li>Other throwable excluding RuntimeException, InterruptedException,
     * ExecutionException and TimeoutException</li>
     * </ul>
     *
     * @param httpClient a HTTP client
     *
     * @param uri the request URI
     *
     * @param headers HTTP headers of this request
     *
     * @param timeoutMs request timeout in milliseconds
     *
     * @param logger logger
     *
     * @return HTTP response, a object encapsulate status code and response
     */
    public static HttpResponse doDeleteRequest(HttpClient httpClient,
                                               String uri,
                                               HttpHeaders headers,
                                               int timeoutMs,
                                               Logger logger) {

        return doRequest(httpClient, uri, headers, DELETE, null,
                         timeoutMs, logger);
    }

    private static HttpResponse doRequest(HttpClient httpClient,
                                          String uri,
                                          HttpHeaders headers,
                                          HttpMethod method,
                                          byte[] payload,
                                          int timeoutMs,
                                          Logger logger) {

        final long startTime = System.currentTimeMillis();
        int numRetries = 0;
        Throwable exception = null;
        HttpResponse res = null;

        do {
            if (numRetries > 0) {
                logInfo(logger, "Client, doing retry: " + numRetries +
                        (exception != null ? ", exception: " + exception : ""));
            }
            ResponseHandler responseHandler = null;
            try {
                final Channel channel = httpClient.getChannel(timeoutMs);
                responseHandler =
                    new ResponseHandler(httpClient, logger, channel);

                FullHttpRequest request;
                if (payload == null) {
                    request = buildRequest(uri, method, headers);
                } else {
                    request = buildRequest(
                        uri, headers, method, payload, channel);
                }
                addRequiredHeaders(request);
                httpClient.runRequest(request, responseHandler, channel);
                if (responseHandler.await(timeoutMs)) {
                    throw new TimeoutException("Request timed out after " +
                        timeoutMs + " milliseconds");
                }

                final HttpResponseStatus status = responseHandler.getStatus();
                if (status == null) {
                    throw new IllegalStateException("Invalid null response");
                }
                res = processResponse(status.code(),
                                      responseHandler.getContent());

                /*
                 * Retry upon status code larger than 500, in general,
                 * this indicates server internal error.
                 */
                if (res.getStatusCode() >= 500) {
                    logFine(logger,
                            "Remote server temporarily unavailable," +
                            " status code " + res.getStatusCode() +
                            " , response " + res.getOutput());
                    delay();
                    ++numRetries;
                    continue;
                }
                return res;
            } catch (RuntimeException e) {
                logFine(logger, "Client execute runtime exception: " +
                        e.getMessage());
                throw e;
            } catch (IOException ioe) {
                String name = ioe.getClass().getName();
                logFine(logger, "Client execute IOException, name: " +
                        name + ", message: " + ioe.getMessage());
                /*
                 * An exception in the channel, e.g. the server may have
                 * disconnected. Retry.
                 */
                exception = ioe;
                delay();
                ++numRetries;
                continue;
            } catch (InterruptedException ie) {
                throw new RuntimeException(
                    "Client interrupted exception: ", ie);
            } catch (ExecutionException ee) {
                throw new RuntimeException(
                    "Unable to execute request: ", ee);
            } catch (TimeoutException te) {
                throw new RuntimeException("Timeout exception: ", te);
            } catch (Throwable t) {
                /*
                 * this is likely an exception from Netty, perhaps a bad
                 * connection. Retry.
                 */
                String name = t.getClass().getName();
                logFine(logger, "Client execute Throwable, name: " +
                        name + "message: " + t.getMessage());

                exception = t;
                delay();
                ++numRetries;
                continue;
            } finally {
                if (responseHandler != null) {
                    responseHandler.close();
                }
            }
        } while ((System.currentTimeMillis()- startTime) < timeoutMs);

        throw new RequestTimeoutException(timeoutMs,
            "Request timed out after " + numRetries +
            (numRetries == 1 ? " retry." : " retries."),
            exception);
    }

    private static FullHttpRequest buildRequest(String requestURI,
                                                HttpMethod method,
                                                HttpHeaders headers) {
        final FullHttpRequest request =
            new DefaultFullHttpRequest(HTTP_1_1, method, requestURI);
        request.headers().add(headers);
        return request;
    }

    private static FullHttpRequest buildRequest(String requestURI,
                                                HttpHeaders headers,
                                                HttpMethod method,
                                                byte[] payload,
                                                Channel channel) {
        final ByteBuf buffer = channel.alloc().directBuffer();
        buffer.writeBytes(payload);

        final FullHttpRequest request =
            new DefaultFullHttpRequest(HTTP_1_1, method, requestURI,
                                       buffer,
                                       false /* Don't validate hdrs */);
        request.headers().add(headers);
        request.headers().setInt(CONTENT_LENGTH, buffer.readableBytes());
        return request;
    }

    /*
     * For now, it's just the Host
     */
    private static void addRequiredHeaders(FullHttpRequest request) {
        try {
            final String host = new URL(request.uri()).getHost();
            request.headers().add(HttpHeaderNames.HOST, host);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                "Bad URL: " + request.uri());
        }
    }

    private static void delay() {
        try {
            Thread.sleep(DEFAULT_DELAY_MS);
        } catch (InterruptedException ie) {}
    }

    /*
     * A simple response processing method, just return response content
     * in String with its status code.
     */
    private static HttpResponse processResponse(int status, ByteBuf content) {
        String output = null;
        if (content != null) {
            output = content.toString(utf8);
        }
        return new HttpResponse(status, output);
    }

    /**
     * Class to package HTTP response output and status code.
     */
    public static class HttpResponse {
        private final int statusCode;
        private final String output;

        public HttpResponse(int statusCode, String output) {
            this.statusCode = statusCode;
            this.output = output;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getOutput() {
            return output;
        }

        @Override
        public String toString() {
            return "HttpResponse [statusCode=" + statusCode + "," +
                   "output=" + output + "]";
        }
    }
}
