package oracle.nosql.driver.httpclient;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClientResponse;

public class HttpResponse {
    private final HttpClientResponse reactorNettyResponse;
    private final Connection reactorNettyConnection;


    HttpResponse(HttpClientResponse reactorNettyResponse, Connection reactorNettyConnection) {
        this.reactorNettyResponse = reactorNettyResponse;
        this.reactorNettyConnection =  reactorNettyConnection;
    } 

    public int getStatusCode() {
        return reactorNettyResponse.status().code();
    }

    public HttpResponseStatus getStatus() {
        return reactorNettyResponse.status();
    }

    public HttpHeaders getHeaders() {
        return reactorNettyResponse.responseHeaders();
    }

    public Mono<String> getBodyAsString() {
        return reactorNettyConnection.inbound().receive().aggregate().asString();
    }

    public String getBodyAsStringSync() {
        return getBodyAsString().block();
    }
    public Mono<ByteBuf> getBody() {
        return reactorNettyConnection.inbound().receive().aggregate();
    }

}
