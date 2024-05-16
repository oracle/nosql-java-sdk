/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.*;
import static oracle.nosql.driver.util.HttpConstants.*;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Set;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import oracle.nosql.driver.httpclient.HttpResponse;
import oracle.nosql.driver.httpclient.ReactorHttpClient;
import oracle.nosql.driver.iam.CertificateSupplier.X509CertificateKeyPair;

import com.fasterxml.jackson.core.JsonGenerator;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * @hidden
 * Internal use only
 */
class FederationRequestHelper {
    /* signing headers used to obtain security token */
    private static final String SIGNING_HEADERS =
        "date (request-target) content-length content-type x-content-sha256";
    private static final String APP_JSON = "application/json";
    private static final String DEFAULT_FINGERPRINT = "SHA256";

    static String getSecurityToken(ReactorHttpClient client,
                                   URI endpoint,
                                   int timeoutMs,
                                   String tenantId,
                                   X509CertificateKeyPair pair,
                                   String body,
                                   Logger logger) {
        CharBuffer charBuf = CharBuffer.wrap(body);
        ByteBuffer buf = StandardCharsets.UTF_8.encode(charBuf);
        byte[] payloadByte = new byte[buf.remaining()];
        buf.get(payloadByte);
        ByteBuf payload = Unpooled.wrappedBuffer(payloadByte);

        HttpResponse response = client.postRequest(
            endpoint.toString(),
            headers(tenantId, endpoint, payload.array(), pair, logger),
                 payload /*,timeoutMs, logger*/).block();

        int responseCode = response.getStatusCode();
        String responseBody = response.getBodyAsStringSync();
        if (responseCode > 299) {
            throw new IllegalStateException(
                String.format(
                    "Error getting security token from IAM, " +
                    "status code %d, response \n%s",
                    response.getStatusCode(),
                    responseBody));
        }
        logTrace(logger, "Federation response " + responseBody);
        return parseTokenResponse(responseBody);
    }

    /*
     * Request body:
     * {
     *  "intermediateCertificates": [
     *    "interCert1",
     *    "interCert3",
     *    "interCert2"
     *  ],
     *  "certificate": "certificate",
     *  "publicKey": "publicKey",
     *  "purpose": "DEFAULT",
     *  "fingerprintAlgorithm", "SHA256"
     * }
     */
    static String getFederationRequestBody(String publicKey,
                                           String certificate,
                                           Set<String> interCerts,
                                           String purpose) {

        try {
            StringWriter sw = new StringWriter();
            try (JsonGenerator gen = createGenerator(sw)) {
                gen.writeStartObject();
                gen.writeStringField("publicKey", publicKey);
                gen.writeStringField("certificate", certificate);
                gen.writeStringField("purpose", purpose);
                gen.writeStringField("fingerprintAlgorithm",
                    DEFAULT_FINGERPRINT);

                gen.writeFieldName("intermediateCertificates");
                gen.writeStartArray();
                for (String interCert : interCerts) {
                    gen.writeString(interCert);
                }
                gen.writeEndArray();
                gen.writeEndObject();
            }

            return sw.toString();
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "Error getting federation request body", ioe);
        }
    }

    private static HttpHeaders headers(String tenantId,
                                       URI endpoint,
                                       byte[] body,
                                       X509CertificateKeyPair pair,
                                       Logger logger) {

        String date = createFormatter().format(new Date());
        String bodySha = computeBodySHA256(body);
        StringBuilder sign = new StringBuilder();
        sign.append(DATE).append(HEADER_DELIMITER)
            .append(date).append("\n")
            .append(REQUEST_TARGET).append(HEADER_DELIMITER)
            .append("post ").append(endpoint.getPath()).append("\n")
            .append(CONTENT_LENGTH.toLowerCase()).append(HEADER_DELIMITER)
            .append(Integer.toString(body.length)).append("\n")
            .append(CONTENT_TYPE.toLowerCase()).append(HEADER_DELIMITER)
            .append(APP_JSON).append("\n")
            .append(CONTENT_SHA).append(HEADER_DELIMITER)
            .append(bodySha);

        logTrace(logger, "Federation request signing content " +
                 sign.toString());
        String signature;
        try {
             signature = sign(sign.toString(), pair.getKey());
        } catch (Exception e) {
            return null;
        }
        String authHeader = String.format(
            SIGNATURE_HEADER_FORMAT,
            SIGNING_HEADERS,
            keyId(tenantId, pair),
            RSA,
            signature,
            SINGATURE_VERSION);

        logTrace(logger, "Federation request authorization header " +
                 authHeader);
        HttpHeaders headers = new DefaultHttpHeaders();
        return headers
            .set(CONTENT_TYPE.toLowerCase(), APP_JSON)
            .set(CONTENT_SHA, bodySha)
            .set(CONTENT_LENGTH.toLowerCase(), body.length)
            .set(DATE, date)
            .set(AUTHORIZATION.toLowerCase(), authHeader);
    }

    private static String keyId(String tenantId, X509CertificateKeyPair pair) {
        return String.format("%s/fed-x509-sha256/%s",
                             tenantId, Utils.getFingerPrint(pair));
    }
}
