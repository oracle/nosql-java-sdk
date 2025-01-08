/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;

import java.net.URI;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;
import javax.security.auth.RefreshFailedException;
import javax.security.auth.Refreshable;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.httpclient.HttpClient;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * @hidden
 * Internal use only
 * <p>
 * This class gets a security token from IAM by signing the request with a PKI
 * issued leaf certificate, passing along a temporary public key that is
 * bounded to the the security token, and the leaf certificate.<p>
 *
 * Reference to the OCI SDK for Java
 * <code>com.oracle.bmc.auth.internal X509FederationClient</code>
 */
class SecurityTokenSupplier {
    private String tenantId;
    private int timeoutMS;
    private URI federationURL;
    private HttpClient federationClient;
    private final CertificateSupplier leafCertSupplier;
    private final Set<CertificateSupplier> intermediateCertificateSuppliers;
    private final SessionKeyPairSupplier keyPairSupplier;
    private final String purpose;
    private long minTokenLifetime;
    private final Logger logger;

    SecurityTokenSupplier(String federationEndpoint,
                          String tenantId,
                          CertificateSupplier leafCertSupplier,
                          Set<CertificateSupplier> interCertificateSuppliers,
                          SessionKeyPairSupplier keyPairSupplier,
                          String purpose,
                          int timeoutMS,
                          Logger logger) {
        this.federationURL = URI.create(federationEndpoint + "/v1/x509");
        requireNonNullIAE(leafCertSupplier,
                          "Certificate supplier cannot be null");
        this.leafCertSupplier = leafCertSupplier;
        requireNonNullIAE(keyPairSupplier,
                          "Keypair supplier cannot be null");
        this.keyPairSupplier = keyPairSupplier;
        this.intermediateCertificateSuppliers = interCertificateSuppliers;
        requireNonNullIAE(tenantId, "Tenant id cannot be null");
        this.tenantId = tenantId;
        requireNonNullIAE(purpose, "Purpose cannot be null");
        this.purpose = purpose;
        this.timeoutMS = timeoutMS;
        this.logger = logger;
    }

    String getSecurityToken() {
        return refreshAndGetTokenInternal();
    }

    void setMinTokenLifetime(long lifetimeMS) {
        this.minTokenLifetime = lifetimeMS;
    }

    void close() {
        if (federationClient != null) {
            federationClient.shutdown();
        }
    }

    synchronized void prepare(NoSQLHandleConfig config) {
        if (federationClient == null) {
            federationClient = buildHttpClient(
                federationURL,
                config.getSslContext(),
                config.getSSLHandshakeTimeout(), logger);
        }
    }

    private static HttpClient buildHttpClient(URI endpoint,
                                              SslContext sslCtx,
                                              int sslHandshakeTimeout,
                                              Logger logger) {
        String scheme = endpoint.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException(
                "Unable to find URL scheme, invalid URL " +
                 endpoint.toString());
        }
        if (scheme.equalsIgnoreCase("http")) {
            return HttpClient.createMinimalClient(endpoint.getHost(), endpoint.getPort(),
                                                  null, 0, "FederationClient", logger);
        }

        if (sslCtx == null) {
            try {
                sslCtx = SslContextBuilder.forClient().build();
            } catch (SSLException se) {
                throw new IllegalStateException(
                    "Unable to build SSL context for http client", se);
            }
        }

        return HttpClient.createMinimalClient(endpoint.getHost(), 443,
                              sslCtx, sslHandshakeTimeout,
                              "FederationClient", logger);
    }

    private synchronized String refreshAndGetTokenInternal() {
        keyPairSupplier.refreshKeys();
        if (leafCertSupplier instanceof Refreshable) {
            try {
                ((Refreshable) leafCertSupplier).refresh();
            } catch (RefreshFailedException rfe) {
                throw new IllegalStateException(
                    "Can't refresh the leaf certification!", rfe);
            }

            String newTenantId = Utils.getTenantId(
                leafCertSupplier
                .getCertificateAndKeyPair()
                .getCertificate());
            logTrace(logger, "Tenant id in refreshed certificate " +
                     newTenantId);

            if (!tenantId.equals(newTenantId)) {
                throw new IllegalArgumentException(
                    "The tenant id should never be changed in certificate");
            }
        }

        for (CertificateSupplier supplier :
             intermediateCertificateSuppliers) {

            if (supplier instanceof Refreshable) {
                try {
                    ((Refreshable) supplier).refresh();
                } catch (RefreshFailedException e) {
                    throw new IllegalStateException(
                        "Can't refresh the intermediate certification!", e);
                }
            }
        }
        return getSecurityTokenFromIAM();
    }

    private String getSecurityTokenFromIAM() {
        KeyPair keyPair = keyPairSupplier.getKeyPair();
        requireNonNullIAE(keyPair, "Keypair for session not provided");

        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        requireNonNullIAE(publicKey, "Public key not present");

        CertificateSupplier.X509CertificateKeyPair certificateAndKeyPair =
            leafCertSupplier.getCertificateAndKeyPair();
        requireNonNullIAE(certificateAndKeyPair,
                          "Certificate and key pair not present");

        X509Certificate leafCertificate = certificateAndKeyPair.getCertificate();
        requireNonNullIAE(leafCertificate,
                          "Leaf certificate not present");
        requireNonNullIAE(certificateAndKeyPair.getKey(),
                          "Leaf certificate's private key not present");

        try {
            Set<String> intermediateStrings = null;
            if (intermediateCertificateSuppliers != null &&
                intermediateCertificateSuppliers.size() > 0) {

                intermediateStrings = new HashSet<>();
                for (CertificateSupplier supplier :
                     intermediateCertificateSuppliers) {

                    CertificateSupplier.X509CertificateKeyPair pair =
                        supplier.getCertificateAndKeyPair();
                    if (pair != null && pair.getCertificate() != null) {
                        intermediateStrings.add(Utils.base64EncodeNoChunking(
                            pair));
                    }
                }
            }

            String securityToken = getSecurityTokenInternal(
                Utils.base64EncodeNoChunking(publicKey),
                Utils.base64EncodeNoChunking(certificateAndKeyPair),
                intermediateStrings);

            SecurityToken st = new SecurityToken(securityToken, keyPairSupplier);
            st.validate(minTokenLifetime, logger);
            return st.getSecurityToken();
        } catch (Exception e) {
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
        }
    }

    private String getSecurityTokenInternal(String publicKey,
                                            String leafCertificate,
                                            Set<String> interCerts) {

        String body = FederationRequestHelper.getFederationRequestBody(
            publicKey, leafCertificate, interCerts, purpose);
        logTrace(logger, "Federation request body " + body);
        return FederationRequestHelper.getSecurityToken(
            federationClient, federationURL, timeoutMS, tenantId,
            leafCertSupplier.getCertificateAndKeyPair(),
            body, logger);
    }

    static class SecurityToken {
        private final SessionKeyPairSupplier keyPairSupplier;
        private final String securityToken;
        private final Map<String, String> tokenClaims;
        private final long creationTime;
        private long expiryMS = -1;

        SecurityToken(String token, SessionKeyPairSupplier supplier) {
            this.securityToken = token;
            this.keyPairSupplier = supplier;
            this.creationTime = System.currentTimeMillis();
            this.tokenClaims = Utils.parseToken(token);
        }

        String getSecurityToken() {
            return securityToken;
        }

        String getStringClaim(String key) {
            return tokenClaims.get(key);
        }

        long getCreationTime() {
            return creationTime;
        }

        long getExpiryMS() {
            return expiryMS;
        }

        /*
         * Validate the token.
         * Throws IAE if any validation fails.
         */
        void validate(long minTokenLifetime, Logger logger) {
            if (tokenClaims == null) {
                throw new IllegalArgumentException(
                    "No security token cached");
            }

            if (expiryMS == -1) {
                String exp = tokenClaims.get("exp");
                if (exp != null) {
                    expiryMS = Long.parseLong(exp) * 1000;
                }
                if (exp == null || expiryMS == -1) {
                    throw new IllegalArgumentException(
                        "No expiration info in token:\n" + securityToken);
                }
            }

            /*
             * Note: expiry check removed, as some tokens may have very short
             * expirations, but allow logging of token expiration details
             */
            long tokenLifetime = getExpiryMS() - getCreationTime();
            logTrace(logger, "New security token: lifetime=" + tokenLifetime +
                     ", expiry=" + getExpiryMS() + ", creation=" +
                     getCreationTime());
            if (tokenLifetime < minTokenLifetime) {
                logTrace(logger, "Security token has shorter lifetime" +
                         "than expected minimal token lifetime, token:\n" +
                         getSecurityToken());
            }

            /*
             * Next compare the public key inside the JWT is the same
             * from the supplier. We check this in case instance secrets
             * service deploys a new key and the JWT is still not expired.
             */
            String modulus = tokenClaims.get("n");
            String exponent = tokenClaims.get("e");

            if (modulus == null) {
                throw new IllegalArgumentException(
                    "Invalid JWK, no modulus in token:\n" +
                    securityToken);
            }
            if (exponent == null) {
                throw new IllegalArgumentException(
                    "Invalid JWK, no exponent in token:\n" +
                    securityToken);
            }
            RSAPublicKey jwkRsa = Utils.toPublicKey(modulus, exponent);
            RSAPublicKey expected = (RSAPublicKey)
                keyPairSupplier.getKeyPair().getPublic();

            if (jwkRsa == null) {
                throw new IllegalArgumentException(
                    "Invalid JWK, unable to find public key in token:\n" +
                    securityToken);
            }
            if (!isEqualPublicKey(jwkRsa, expected)) {
                throw new IllegalArgumentException(
                    "Invalid JWK, public key not match in token:\n" +
                    securityToken);
            }
        }

        /**
         * Checks if two public keys are equal
         * @param actual one public key
         * @param expect the other one
         * @return true if the same
         */
        private boolean isEqualPublicKey(RSAPublicKey actual,
                                         RSAPublicKey expect) {
            if (actual == null || expect == null) {
                throw new IllegalArgumentException("Public key cannot be null");
            }

            String actualKey = Utils.base64EncodeNoChunking(actual);
            String expectKey = Utils.base64EncodeNoChunking(expect);

            return actualKey.equals(expectKey);
        }
    }

    /**
     * @hidden
     * Internal use only
     */
    public interface SecurityTokenBasedProvider {

        /**
         * @hidden
         * Set the expected minimal token lifetime in milliseconds.
         *
         * The expected minimal lifetime is configured by SignatureProvider,
         * the same as the signature cache duration. Security token providers
         * should validate token to ensure it has the expected minimal lifetime,
         * throw an error otherwise.
         */
        void setMinTokenLifetime(long lifeTimeMS);

        /**
         * @hidden
         * Prepare the provider. This method can be used to configure the
         * service URL, does http client warm-up. This method is usually
         * called when the NoSQLHandle is created. Does nothing by default.
         */
        default void prepare(NoSQLHandleConfig config) {
        }

        /**
         * @hidden
         * Close the provider. This method can be used to cleanup the resources
         * used by the provider, such as close the http client. This method
         * is usually called when the NoSQLHandle is closed. Does nothing by
         * default.
         */
        default void close() {
        }
    }
}
