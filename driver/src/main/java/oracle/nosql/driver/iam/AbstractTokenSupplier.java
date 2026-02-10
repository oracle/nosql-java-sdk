/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.isNotBlank;
import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.iam.Utils.parseTokenResponse;
import static oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;
import static oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.util.Objects;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.util.HttpRequestUtil;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

abstract class AbstractTokenSupplier
        implements TokenSupplier{

    private static final int DEFAULT_TIMEOUT_MS = 5_000;
    protected static final int timeoutMs = DEFAULT_TIMEOUT_MS;
    protected final SessionKeyPairSupplier sessionKeyPairSupplier;
    protected HttpClient resourcePrincipalTokenClient;
    protected HttpClient federationClient;
    protected URI resourcePrincipalTokenURI;
    protected final URI federationURI;
    private volatile SecurityToken securityToken;
    private long minTokenLifetime;
    protected String keyId;
    protected PrivateKeyProvider privateKeyProvider;
    private final Logger logger;

    public AbstractTokenSupplier(
            String resourcePrincipalTokenEndpoint,
            String federationEndpoint,
            String resourcePrincipalTokenPath,
            SessionKeyPairSupplier sessionKeySupplier,
            Logger logger
    ){
        this (
                resourcePrincipalTokenEndpoint + resourcePrincipalTokenPath,
                federationEndpoint,
                sessionKeySupplier,
                logger
        );
    }

    public AbstractTokenSupplier(
            String resourcePrincipalTokenUrl,
            String federationEndpoint,
            SessionKeyPairSupplier sessionKeySupplier,
            Logger logger
    ){
        Objects.requireNonNull(resourcePrincipalTokenUrl,
                "resourcePrincipalTokenEndpoint cannot be null");
        Objects.requireNonNull(federationEndpoint,
                "federationEndpoint");
        Objects.requireNonNull(sessionKeySupplier,
                "sessionKeySupplier cannot be null");

        this.sessionKeyPairSupplier = sessionKeySupplier;
        this.logger = logger;
        this.resourcePrincipalTokenURI = URI.create(resourcePrincipalTokenUrl);
        this.federationURI =
                URI.create(federationEndpoint + "/v1/resourcePrincipalSessionToken");
        this.resourcePrincipalTokenClient =
                buildHttpClient(resourcePrincipalTokenURI,
                        "resourcePrincipalTokenClient", logger);
        this.federationClient =
                buildHttpClient(federationURI, "federationClient",logger);
    }

    /**
     * Return the security token of resource principal.
     */
    @Override
    public String getSecurityToken() {
        return refreshAndGetSecurityToken();
    }

    @Override
    public void prepare(NoSQLHandleConfig config) {
    }

    /**
     * Cleans up the resources held by the token supplier
     */
    @Override
    public synchronized void close() {
        if (resourcePrincipalTokenClient != null) {
            resourcePrincipalTokenClient.shutdown();
        }
        if (federationClient != null) {
            federationClient.shutdown();
        }
    }

    /**
     * Set expected minimal token lifetime.
     */
    @Override
    public void setMinTokenLifetime(long lifetimeMS) {
        this.minTokenLifetime = lifetimeMS;
    }

    /**
     * Return the specific claim in security token by given key.
     */
    @Override
    public String getStringClaim(String key) {
        if (securityToken == null) {
            refreshAndGetSecurityToken();
        }
        return securityToken.getStringClaim(key);
    }

    protected static HttpClient buildHttpClient(URI endpoint,
                                              String clientName,
                                              Logger logger) {
        String scheme = endpoint.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException(
                    "Unable to find URL scheme, invalid URL " +
                            endpoint.toString());
        }
        if (scheme.equalsIgnoreCase("http")) {
            return HttpClient.createMinimalClient(endpoint.getHost(), endpoint.getPort(),
                    null, 0, clientName, logger);
        }

        SslContext sslCtx;
        try {
            sslCtx = SslContextBuilder.forClient().build();
        } catch (SSLException se) {
            throw new IllegalStateException(
                    "Unable to build SSL context for http client", se);
        }

        return HttpClient.createMinimalClient(endpoint.getHost(), 443,
                sslCtx, 0,
                clientName, logger);
    }

    protected synchronized String refreshAndGetSecurityToken() {
        logTrace(logger, "Refreshing session keys");
        sessionKeyPairSupplier.refreshKeys();

        try {
            logTrace(logger, "Getting security token from Federation Server.");
            SecurityTokenSupplier.SecurityToken token = getSecurityTokenFromServer();
            token.validate(minTokenLifetime, logger);
            securityToken = token;
            return securityToken.getSecurityToken();
        } catch (Exception e) {
            close(); // release the resources held by http clients
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
        }
    }

    /**
     * Gets a security token from the federation server
     *
     * @return the security token, which is basically a JWT token string
     */
    protected abstract SecurityToken getSecurityTokenFromServer();

    protected HttpResponse getResourcePrincipalTokenResponse(
            String securityContext) {

        HttpHeaders resourcePrincipalTokenHeader = FederationRequestHelper.setHeaders(
                resourcePrincipalTokenURI,
                keyId,
                privateKeyProvider.getKey(),
                logger);

        if (isNotBlank(securityContext)) {
            resourcePrincipalTokenHeader.set("security-context", securityContext);
        }

        // Fetch Resource Principal Token from the Resource's control plane
        HttpResponse resourcePrincipalTokenResponse =
                HttpRequestUtil.doGetRequest(
                        resourcePrincipalTokenClient,
                        resourcePrincipalTokenURI.toString(),
                        resourcePrincipalTokenHeader,
                        timeoutMs,
                        logger);

        int status = resourcePrincipalTokenResponse.getStatusCode();
        if(status != 200) {
            throw new IllegalStateException(
                    String.format("Unable to fetch Resource Principal Token " +
                                    "from resource control plane endpoint - %s" +
                                    ", status code: %d, output: %s",
                            resourcePrincipalTokenURI.getHost(),
                            resourcePrincipalTokenResponse.getStatusCode(),
                            resourcePrincipalTokenResponse.getOutput()));
        }

        return resourcePrincipalTokenResponse;
    }

    protected SecurityToken getResourcePrincipalSessionToken(
            String resourcePrincipalToken,
            String servicePrincipalSessionToken,
            String publicKey
    ) {
        String payload =
                FederationRequestHelper.getResourcePrincipalSessionTokenRequestBody(
                        resourcePrincipalToken,
                        servicePrincipalSessionToken,
                        publicKey);

        byte[] payloadBytes = payload.getBytes();

        // Get Resource principal session token from Identity data plane
        HttpRequestUtil.HttpResponse resourcePrincipalSessionTokenResponse =
                HttpRequestUtil.doPostRequest(
                        federationClient,
                        federationURI.toString(),
                        FederationRequestHelper.setHeaders(
                                federationURI,
                                payloadBytes,
                                keyId,
                                privateKeyProvider.getKey(),
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
                            federationURI.getHost(),
                            resourcePrincipalSessionTokenResponse.getStatusCode(),
                            resourcePrincipalSessionTokenResponse.getOutput()));
        }

        String resourcePrincipalSessionToken = parseTokenResponse(
                resourcePrincipalSessionTokenResponse.getOutput());

        return new SecurityToken(
                resourcePrincipalSessionToken, sessionKeyPairSupplier);
    }
}
