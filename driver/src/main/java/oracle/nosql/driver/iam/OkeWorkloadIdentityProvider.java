/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;
import static oracle.nosql.driver.util.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.util.HttpRequestUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * @hidden
 * Internal use only
 * <p>
 * The provider used to authenticate with OKE workload identity.
 */
class OkeWorkloadIdentityProvider
    implements AuthenticationProfileProvider,
                Region.RegionProvider,
                SecurityTokenSupplier.SecurityTokenBasedProvider {

    /* Default path for reading Kubernetes service account cert */
    private static final String DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH =
        "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

    /* Environment variable of the path for Kubernetes service account cert */
    private static final String KUBERNETES_SERVICE_ACCOUNT_CERT_PATH_ENV =
        "OCI_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH";

    /* Environment variable of Kubernetes service proxymux host */
    private static final String KUBERNETES_SERVICE_HOST =
        "KUBERNETES_SERVICE_HOST";

    /* Environment variable that may contain instance/region metadata:
     * format is JSON if it is set:
     * {
     *  "realmKey":"OC1",
     *  "realmDomainComponent":"oraclecloud.com",
     *  "regionKey":"IAD",
     *  "regionIdentifier":"us-ashburn-1"
     * }
     */
    private static final String OCI_REGION_METADATA = "OCI_REGION_METADATA";
    private static final String REGION_ID = "regionIdentifier";

    /* Kubernetes proxymux port */
    private static final int PROXYMUX_SERVER_PORT = 12250;
    private static final String OPC_REQUEST_ID_HEADER = "opc-request-id";
    private static final String JWT_FORMAT = "Bearer %s";
    private static final int timeoutMs = 5_000;

    /* Default path for service account token */
    static final String KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH =
        "/var/run/secrets/kubernetes.io/serviceaccount/token";
    static final String TOKEN_PATH = "/resourcePrincipalSessionTokens";

    private final DefaultSessionKeySupplier sessionKeySupplier;
    private final String serviceAccountToken;
    private final File serviceAccountTokenFile;
    private final URI tokenURL;
    private final Region region;
    private final Logger logger;
    private long minTokenLifetime;
    private HttpClient okeTokenClient;

    /**
     * Create an OkeWorkloadIdentityProvider. The provider can be created with a
     * Kubernetes service account token string or token file. If none of them is
     * specified, the provider will be created with the default token file.
     *
     * @param serviceAccountToken the Kubernetes service account token in string
     * @param serviceAccountTokenFile the Kubernetes service account token file
     * @param logger logger
     */
    OkeWorkloadIdentityProvider(String serviceAccountToken,
                                File serviceAccountTokenFile,
                                Logger logger) {
        if (serviceAccountToken != null && serviceAccountTokenFile != null) {
            throw new IllegalArgumentException(
                "Must not specify both service account token string and file");
        }
        this.serviceAccountToken = serviceAccountToken;
        this.serviceAccountTokenFile = (serviceAccountTokenFile != null) ?
            serviceAccountTokenFile :
            new File(KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH);
        this.sessionKeySupplier = new DefaultSessionKeySupplier(
            new SessionKeyPairSupplier.JDKKeyPairSupplier());
        this.logger = (logger != null) ?
            logger : Logger.getLogger(getClass().getName());
        this.tokenURL = getURL();
        this.region = getRegionFromInstance(logger);
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public String getKeyId() {
        return "ST$" + getSecurityToken();
    }

    @Override
    public InputStream getPrivateKey() {
        return new ByteArrayInputStream(sessionKeySupplier.getPrivateKeyBytes());
    }

    @Override
    public char[] getPassphraseCharacters() {
        return null;
    }

    @Override
    public void setMinTokenLifetime(long lifeTimeMS) {
        this.minTokenLifetime = lifeTimeMS;
    }

    @Override
    public void prepare(NoSQLHandleConfig config) {
        final File kubernetesCaCertFile = new File(
            System.getenv(KUBERNETES_SERVICE_ACCOUNT_CERT_PATH_ENV) != null ?
            System.getenv(KUBERNETES_SERVICE_ACCOUNT_CERT_PATH_ENV) :
            DEFAULT_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH);

        if (!kubernetesCaCertFile.exists()) {
            throw new IllegalArgumentException(
                "Kubernetes service account ca cert doesn't exist.");
        }

        /*
         * OKE HttpClient use dedicated ca cert and disable
         * endpoint identification/verification.
         */
        final SslContext sslCtx;
        try {
            sslCtx = SslContextBuilder.forClient()
                .trustManager(kubernetesCaCertFile)
                .build();
        } catch (SSLException se) {
            throw new IllegalArgumentException(
                "Error building SSL context from " +
                "Kubernetes service account ca cert ", se);
        }
        this.okeTokenClient = HttpClient.createMinimalClient(
            tokenURL.getHost(), tokenURL.getPort(),
            sslCtx, config.getSSLHandshakeTimeout(),
            "OkeWorkloadIdentityResourcePrincipalsTokenClient", logger);
        okeTokenClient.disableEndpointIdentification();
    }

    @Override
    public void close() {
        if (okeTokenClient != null) {
            okeTokenClient.shutdown();
        }
    }

    private static URI getURL() {
        /* Read proxymux ip address from environment variable */
        final String host = System.getenv().get(KUBERNETES_SERVICE_HOST);
        if (host == null) {
            throw new IllegalArgumentException(
                "Invalid environment variable KUBERNETES_SERVICE_HOST, " +
                "please contact OKE Foundation team for help.");
        }

        /* Set proxymux endpoint port for requesting rpst token */
        return URI.create("https://" + host + ":" + PROXYMUX_SERVER_PORT +
                          TOKEN_PATH);
    }

    private String getSecurityToken() {
        sessionKeySupplier.refreshKeys();
        final String saToken = getServiceAccountToken();
        final KeyPair keyPair = sessionKeySupplier.getKeyPair();
        requireNonNullIAE(keyPair, "Keypair for session not provided");
        final RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        requireNonNullIAE(publicKey, "Public key not present");

        final String requestId = Utils.generateOpcRequestId();
        Utils.logTrace(logger, "opc-request-id of token request: " + requestId);
        final String body = getRequestBody(publicKey);
        final CharBuffer charBuf = CharBuffer.wrap(body);
        final ByteBuffer buf = StandardCharsets.UTF_8.encode(charBuf);
        final byte[] payloadByte = new byte[buf.remaining()];
        buf.get(payloadByte);

        try {
            return getSecurityTokenFromProxymux(requestId, saToken, payloadByte);
        } catch (Exception e) {
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
        }
    }

    private String getSecurityTokenFromProxymux(String requestId,
                                                String saToken,
                                                byte[] payloadByte) {
        HttpRequestUtil.HttpResponse response = HttpRequestUtil.doPostRequest(
            okeTokenClient, tokenURL.toString(), headers(saToken, requestId),
            payloadByte, timeoutMs, logger);

        final int responseCode = response.getStatusCode();
        String responseOutput = response.getOutput();
        if (responseCode > 299) {
            throw new IllegalStateException(
                String.format(
                    "Error getting security token from Kubernetes proxymux, " +
                    "status code %d, opc-request-id %s, response\n%s",
                    responseCode, requestId, responseOutput));
        }

        /* The encoded response is returned with quotation marks */
        responseOutput = responseOutput.replace("\"", "");
        Utils.logTrace(logger, "Token response " + responseOutput);
        final String decoded = new String(
            Base64.getDecoder().decode(responseOutput),
            StandardCharsets.UTF_8);
        Utils.logTrace(logger, "Decoded Token " + decoded);

        /*
         * Kubernetes token has duplicated key id prefix "ST$" in the token
         * remove it for token validation.
         */
        final String token = Utils.parseTokenResponse(decoded).substring(3);
        final SecurityTokenSupplier.SecurityToken securityToken =
            new SecurityTokenSupplier.SecurityToken(token, sessionKeySupplier);
        securityToken.validate(minTokenLifetime, logger);
        return securityToken.getSecurityToken();
    }

    private String getServiceAccountToken() {
        String token = serviceAccountToken;
        if (token == null) {
            try {
                token = new String(Files.readAllBytes(
                    serviceAccountTokenFile.toPath()));
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Kubernetes service account token unavailable.", ioe);
            }
        }

        /* Expiration validation */
        final Map<String, String> tokenClaims = Utils.parseToken(token);
        final String exp = tokenClaims.get("exp");
        long expiryMS = -1;
        if (exp != null) {
            expiryMS = Long.parseLong(exp) * 1000;
        }
        if (exp == null || expiryMS == -1) {
            throw new IllegalArgumentException(
                "No expiration info in Kubernetes service account:\n" + token);
        }
        if (expiryMS < System.currentTimeMillis()) {
            throw new IllegalArgumentException(
                "Kubernetes service account token expired.");
        }
        return token;
    }

    private static String getRequestBody(RSAPublicKey publicKey) {
        try {
            final String podKey = new String(Base64.getEncoder().encode(
                publicKey.getEncoded()), StandardCharsets.UTF_8);
            final StringWriter sw = new StringWriter();
            try (JsonGenerator gen = Utils.createGenerator(sw)) {
                gen.writeStartObject();
                gen.writeStringField("podKey", podKey);
                gen.writeEndObject();
            }

            return sw.toString();
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "Error getting federation request body", ioe);
        }
    }

    private static HttpHeaders headers(String token, String opcRequestId) {
        final HttpHeaders headers = new DefaultHttpHeaders();
        return headers
            .set(CONTENT_TYPE, APPLICATION_JSON)
            .set(OPC_REQUEST_ID_HEADER, opcRequestId)
            .set(AUTHORIZATION, String.format(JWT_FORMAT, token));
    }

    private static Region getRegionFromInstance(Logger logger) {
        String regionId = null;
        /*
         * A number of serverless services (OMK, Container instances,
         * virtual nodes, ...) do not have an IMDS (Instance Metadata Service)
         * which allows fetching of instance info from a URL. These instead
         * use the OCI_REGION_METADATA environment variable to inject this
         * information into the runtime environment. Check that first.
         */
        String regionMetadata = System.getenv(OCI_REGION_METADATA);
        if (regionMetadata != null) {
            /* use value classes as convenience to parse and use JSON */
            FieldValue jsonMetadata =
                FieldValue.createFromJson(regionMetadata, null);
            if (!(jsonMetadata instanceof MapValue)) {
                throw new IllegalArgumentException(
                    "Invalid format of OCI_REGION_METADATA: " + regionMetadata);
            }
            regionId = ((MapValue)jsonMetadata).getString(REGION_ID);
        } else {
            /* use the IMDS */
            final InstanceMetadataHelper.InstanceMetadata instanceMetadata =
                InstanceMetadataHelper.fetchMetadata(timeoutMs, logger);
            regionId = instanceMetadata.getRegion();
        }
        return Region.fromRegionId(regionId);
    }
}
