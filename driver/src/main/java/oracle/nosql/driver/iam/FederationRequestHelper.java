/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
import java.security.PrivateKey;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.iam.CertificateSupplier.X509CertificateKeyPair;
import oracle.nosql.driver.util.HttpRequestUtil;
import oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;

import com.fasterxml.jackson.core.JsonGenerator;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * @hidden
 * Internal use only
 */
class FederationRequestHelper {
    /* signing headers used to obtain security token */
    private static final String SIGNING_HEADERS_WITH_PAYLOAD =
            "date (request-target) content-length content-type x-content-sha256";
    private static final String SIGNING_HEADERS_WITHOUT_PAYLOAD =
            "date (request-target)";
    private static final String APP_JSON = "application/json";
    private static final String DEFAULT_FINGERPRINT = "SHA256";

    static String getSecurityToken(HttpClient client,
                                   URI endpoint,
                                   int timeoutMs,
                                   String tenantId,
                                   X509CertificateKeyPair pair,
                                   String body,
                                   Logger logger) {
        byte[] payloadByte = Utils.stringToUtf8Bytes(body);

        HttpResponse response = HttpRequestUtil.doPostRequest(
                client,
                endpoint.toString(),
                setHeaders(
                        endpoint,
                        payloadByte,
                        keyId(tenantId, pair),
                        pair.getKey(),
                        logger
                ),
                payloadByte,
                timeoutMs,
                logger);

        int responseCode = response.getStatusCode();
        if (responseCode > 299) {
            throw new IllegalStateException(
                String.format(
                    "Error getting security token from IAM, " +
                    "status code %d, response \n%s",
                    response.getStatusCode(),
                    response.getOutput()));
        }
        logTrace(logger, "Federation response " + response.getOutput());
        return parseTokenResponse(response.getOutput());
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
    static String getInstancePrincipalSessionTokenRequestBody(
            String publicKey,
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

    /*
     * Request body:
     * {
     *  "resourcePrincipalToken": "....",
     *  "servicePrincipalSessionToken": "....",
     *  "sessionPublicKey": "publicKey"
     * }
     */
    static String getResourcePrincipalSessionTokenRequestBody(
            String resourcePrincipalToken,
            String servicePrincipalSessionToken,
            String sessionPublicKey){
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("resourcePrincipalToken", resourcePrincipalToken);
        jsonMap.put("servicePrincipalSessionToken", servicePrincipalSessionToken);
        jsonMap.put("sessionPublicKey", sessionPublicKey);
        return convertMapToJson(jsonMap);
    }

    static HttpHeaders setHeaders(URI uri,
                                  String keyId,
                                  PrivateKey privateKey,
                                  Logger logger) {
        final String date = createFormatter().format(new Date());
        StringBuilder sign = new StringBuilder();
        sign.append(DATE).append(HEADER_DELIMITER)
                .append(date).append("\n")
                .append(REQUEST_TARGET).append(HEADER_DELIMITER)
                .append("get ").append(uri.getPath());

        logTrace(logger, "Resource Principal Token request" +
                " signing content " + sign);

        String signature;
        try {
            signature = sign(sign.toString(), privateKey);
        } catch (Exception e) {
            return null;
        }

        String authHeader = String.format(
                SIGNATURE_HEADER_FORMAT,
                SIGNING_HEADERS_WITHOUT_PAYLOAD,
                keyId,
                RSA,
                signature,
                SINGATURE_VERSION);


        logTrace(logger, "Resource Principal Token request" +
                " authorization header " + authHeader);
        HttpHeaders headers = new DefaultHttpHeaders();
        return headers
                .set(DATE, date)
                .set(AUTHORIZATION.toLowerCase(), authHeader);
    }

    static HttpHeaders setHeaders(URI uri,
                                  byte[] body,
                                  String keyId,
                                  PrivateKey privateKey,
                                  Logger logger) {
        final String date = createFormatter().format(new Date());
        String bodySha = computeBodySHA256(body);
        StringBuilder sign = new StringBuilder();
        sign.append(DATE).append(HEADER_DELIMITER)
                .append(date).append("\n")
                .append(REQUEST_TARGET).append(HEADER_DELIMITER)
                .append("post ").append(uri.getPath()).append("\n")
                .append(CONTENT_LENGTH.toLowerCase()).append(HEADER_DELIMITER)
                .append(body.length).append("\n")
                .append(CONTENT_TYPE.toLowerCase()).append(HEADER_DELIMITER)
                .append(APP_JSON).append("\n")
                .append(CONTENT_SHA).append(HEADER_DELIMITER)
                .append(bodySha);

        logTrace(logger, "Federation Request signing content " +
                sign);
        String signature;
        try {
            signature = sign(sign.toString(), privateKey);
        } catch (Exception e) {
            return null;
        }

        String authHeader = String.format(
                SIGNATURE_HEADER_FORMAT,
                SIGNING_HEADERS_WITH_PAYLOAD,
                keyId,
                RSA,
                signature,
                SINGATURE_VERSION);

        logTrace(logger, "Federation request authorization header " +
                authHeader);
        HttpHeaders headers = new DefaultHttpHeaders();
        return headers
                .set(DATE, date)
                .set(AUTHORIZATION.toLowerCase(), authHeader)
                .set(CONTENT_TYPE.toLowerCase(), APP_JSON)
                .set(CONTENT_SHA, bodySha)
                .set(CONTENT_LENGTH.toLowerCase(), body.length);
    }

    private static String keyId(String tenantId, X509CertificateKeyPair pair) {
        return String.format("%s/fed-x509-sha256/%s",
                             tenantId, Utils.getFingerPrint(pair));
    }
}
