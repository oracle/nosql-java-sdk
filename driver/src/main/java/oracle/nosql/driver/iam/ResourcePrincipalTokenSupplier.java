/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static oracle.nosql.driver.util.HttpConstants.*;
import static oracle.nosql.driver.iam.Utils.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.*;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpMethod.POST;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContextBuilder;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;
import oracle.nosql.driver.util.HttpRequestUtil;

/**
 * @hidden
 * Internal use only
 * <p>
 * The class to supply security token for resource principal
 */
public class ResourcePrincipalTokenSupplier
            extends TokenSupplier{
    private static final Logger logger = Logger.getLogger(ResourcePrincipalTokenSupplier.class.getName());

    /* signing headers used to obtain security token */
    private static final String SIGNING_HEADERS_GET =
            "date (request-target) host";
    private static final String SIGNING_HEADERS_POST =
            "date (request-target) host content-length content-type x-content-sha256";

    private static final String APP_JSON = "application/json";
    private static final int DEFAULT_TIMEOUT_MS = 5_000;

    private static final int timeoutMs = DEFAULT_TIMEOUT_MS;
    private final SessionKeyPairSupplier sessionKeyPairSupplier;
    private final HttpClient resourcePrincipalTokenClient;
    private final HttpClient federationClient;
    private final URL resourcePrincipalTokenURL;
    private final URL rpstFederationURL;
    private final String securityContext;
    private final AuthenticationProfileProvider provider;
    private volatile SecurityTokenSupplier.SecurityToken securityToken;

    public ResourcePrincipalTokenSupplier(
            String resourcePrincipalTokenEndpoint,
            String federationEndpoint,
            String resourcePrincipalTokenPath,
            SessionKeyPairSupplier sessionKeySupplier,
            AuthenticationProfileProvider provider,
            String securityContext
    ){
        Objects.requireNonNull(resourcePrincipalTokenEndpoint,
                "resourcePrincipalTokenEndpoint cannot be null");
        Objects.requireNonNull(federationEndpoint,
                "federationEndpoint");
        Objects.requireNonNull(sessionKeySupplier,
                "sessionKeySupplier cannot be null");

        this.sessionKeyPairSupplier = sessionKeySupplier;
        this.securityContext = securityContext;
        this.provider = provider;

        resourcePrincipalTokenURL = NoSQLHandleConfig.createURL(
                resourcePrincipalTokenEndpoint,
                resourcePrincipalTokenPath);
        rpstFederationURL = NoSQLHandleConfig.createURL(
                federationEndpoint,
                "/v1/resourcePrincipalSessionToken");

        try {
            this.resourcePrincipalTokenClient = HttpClient.createMinimalClient(
                    resourcePrincipalTokenURL.getHost(),
                    resourcePrincipalTokenURL.getPort(),
                    resourcePrincipalTokenURL.getProtocol().equals("https")
                            ? SslContextBuilder.forClient().build()
                            : null,
                    0,
                    "resourcePrincipalTokenClient",
                    logger);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to build Resource Principal Token Client", e);
        }

        try {
            this.federationClient = HttpClient.createMinimalClient(
                    rpstFederationURL.getHost(),
                    rpstFederationURL.getPort(),
                    rpstFederationURL.getProtocol().equals("https")
                            ? SslContextBuilder.forClient().build()
                            : null,
                    0,
                    "federationClient",
                    logger);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to build Federation Client", e);
        }
    }

    /**
     * Return the security token of resource principal.
     */
    @Override
    public String getSecurityToken() {
        return refreshAndGetSecurityToken();
    }

    /**
     * Return the specific claim in security token by given key.
     */
    @Override
    public String getStringClaim(String key) {
        return null;
    }

    protected synchronized String refreshAndGetSecurityToken() {
        logTrace(logger, "Refreshing session keys");
        sessionKeyPairSupplier.refreshKeys();

        try {
            logTrace(logger, "Getting security token from Federation Server.");
            SecurityToken token = getSecurityTokenFromServer();
            token.validate(minTokenLifetime, logger);
            securityToken = token;
            return securityToken.getSecurityToken();
        } catch (Exception e) {
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
        }
    }

    SecurityTokenSupplier.SecurityToken getSecurityTokenFromServer() throws Exception {
        logTrace(logger, "Getting security token from the auth server");

        KeyPair keyPair = sessionKeyPairSupplier.getKeyPair();
        if (keyPair == null) {
            throw new IllegalArgumentException(
                    "Keypair for session was not provided");
        }

        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        if (publicKey == null) {
            throw new IllegalArgumentException("Public key is not present");
        }

        String keyId = provider.getKeyId();
        PrivateKey privateKey = parsePrivateKeyStream(provider.getPrivateKey());

        Map<String, String> resourcePrincipalTokenResponse =
                getResourcePrincipalTokenResponse(keyId, privateKey, securityContext);

        String resourcePrincipalToken =
                resourcePrincipalTokenResponse.get("resourcePrincipalToken");
        String servicePrincipalSessionToken =
                resourcePrincipalTokenResponse.get("servicePrincipalSessionToken");

        System.out.println("\tkeyId :\n" + keyId);
        System.out.println("\tRPT :\n" + resourcePrincipalToken);
        System.out.println("\tSPST :\n" + servicePrincipalSessionToken);

        String resourcePrincipalSessionToken = getResourcePrincipalSessionToken(
                resourcePrincipalToken,
                servicePrincipalSessionToken,
                base64EncodeNoChunking(publicKey),
                keyId,
                privateKey
        );

        return new SecurityTokenSupplier.SecurityToken(
                resourcePrincipalSessionToken, sessionKeyPairSupplier);
    }

    protected Map<String,String> getResourcePrincipalTokenResponse(
            String keyID,
            PrivateKey privateKey,
            String securityContext) throws Exception {

        HttpHeaders resourcePrincipalTokenHeader = FederationRequestHelper.setHeaders(
                resourcePrincipalTokenURL.toURI(),
                keyID,
                privateKey,
                logger);

        if (isNotBlank(securityContext)) {
            resourcePrincipalTokenHeader.set("security-context", securityContext);
        }

        // Fetch Resource Principal Token from the Resource's control plane
        HttpRequestUtil.HttpResponse resourcePrincipalTokenResponse =
                HttpRequestUtil.doGetRequest(
                        resourcePrincipalTokenClient,
                        resourcePrincipalTokenURL.toString(),
                        resourcePrincipalTokenHeader,
                        timeoutMs,
                        logger);

        int status = resourcePrincipalTokenResponse.getStatusCode();
        if(status != 200) {
            throw new IllegalStateException(
                    String.format("Unable to fetch Resource Principal Token " +
                                    "from resource control plane endpoint - %s" +
                                    ", status code: %d, output: %s",
                            resourcePrincipalTokenURL.getHost(),
                            resourcePrincipalTokenResponse.getStatusCode(),
                            resourcePrincipalTokenResponse.getOutput()));
        }

        return parseResourcePrincipalTokenResponse(
                resourcePrincipalTokenResponse.getOutput()
        );
    }

    protected String getResourcePrincipalSessionToken(
            String resourcePrincipalToken,
            String servicePrincipalSessionToken,
            String publicKey,
            String keyId,
            PrivateKey privateKey
    ) throws Exception {

        String payload =
                FederationRequestHelper.getResourcePrincipalSessionTokenRequestBody(
                        resourcePrincipalToken,
                        servicePrincipalSessionToken,
                        publicKey);

        byte[] payloadBytes = payload.getBytes();

        // Get Resource principal session token from Identity
        HttpRequestUtil.HttpResponse resourcePrincipalSessionTokenResponse =
                HttpRequestUtil.doPostRequest(
                        federationClient,
                        rpstFederationURL.toString(),
                        FederationRequestHelper.setHeaders(
                                rpstFederationURL.toURI(),
                                payloadBytes,
                                keyId,
                                privateKey,
                                logger),
                        payloadBytes,
                        timeoutMs,
                        logger);

        int status = resourcePrincipalSessionTokenResponse.getStatusCode();
        if(status != 200) {
            throw new IllegalStateException(
                    String.format("Unable to fetch Resource Principal Session " +
                                    "Token from federation endpoint - %s" +
                                    ", status code: %d, output: %s",
                            rpstFederationURL.getHost(),
                            resourcePrincipalSessionTokenResponse.getStatusCode(),
                            resourcePrincipalSessionTokenResponse.getOutput()));
        }

        return parseTokenResponse(
                resourcePrincipalSessionTokenResponse.getOutput());
    }
}
