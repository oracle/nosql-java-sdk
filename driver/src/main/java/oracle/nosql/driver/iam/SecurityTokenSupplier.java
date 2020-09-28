/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.*;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.RefreshFailedException;
import javax.security.auth.Refreshable;

import oracle.nosql.driver.iam.SignatureProvider.ResourcePrincipalClaimKeys;
import oracle.nosql.driver.util.CheckNull;

import com.fasterxml.jackson.core.JsonParser;

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
    private URI federationEndpoint;
    private final CertificateSupplier leafCertSupplier;
    private final Set<CertificateSupplier> intermediateCertificateSuppliers;
    private final SessionKeyPairSupplier keyPairSupplier;
    private final String purpose;
    private volatile SecurityToken currentToken = null;
    private final Logger logger;

    SecurityTokenSupplier(String federationEndpoint,
                          String tenantId,
                          CertificateSupplier leafCertSupplier,
                          Set<CertificateSupplier> interCertificateSuppliers,
                          SessionKeyPairSupplier keyPairSupplier,
                          String purpose,
                          int timeoutMS,
                          Logger logger) {

        this.federationEndpoint = URI.create(federationEndpoint + "/v1/x509");
        CheckNull.requireNonNull(leafCertSupplier,
                                 "Certificate supplier cannot be null");
        this.leafCertSupplier = leafCertSupplier;
        CheckNull.requireNonNull(keyPairSupplier,
                                 "Keypair supplier cannot be null");
        this.keyPairSupplier = keyPairSupplier;
        this.intermediateCertificateSuppliers = interCertificateSuppliers;
        CheckNull.requireNonNull(tenantId, "Tenant id cannot be null");
        this.tenantId = tenantId;
        CheckNull.requireNonNull(purpose, "Purpose cannot be null");
        this.purpose = purpose;
        this.currentToken = new SecurityToken(null, keyPairSupplier);
        this.timeoutMS = timeoutMS;
        this.logger = logger;
    }

    String getSecurityToken() {
        if (currentToken.isValid(logger)) {
            return currentToken.getSecurityToken();
        }

        return refreshAndGetTokenInternal(true);
    }

    private String refreshAndGetTokenInternal(boolean doFinalValidityCheck) {
        synchronized (this) {
            if (doFinalValidityCheck && currentToken.isValid(logger)) {
                return currentToken.getSecurityToken();
            }

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

            currentToken = getSecurityTokenFromIAM();
            if (!currentToken.isValid(logger)) {
                logTrace(logger, "Token from IAM is not valid");
            }
        }
        return currentToken.getSecurityToken();
    }

    private SecurityToken getSecurityTokenFromIAM() {
        KeyPair keyPair = keyPairSupplier.getKeyPair();
        CheckNull.requireNonNull(keyPair, "Keypair for session not provided");

        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        CheckNull.requireNonNull(publicKey, "Public key not present");

        CertificateSupplier.X509CertificateKeyPair certificateAndKeyPair =
            leafCertSupplier.getCertificateAndKeyPair();
        CheckNull.requireNonNull(certificateAndKeyPair,
                                 "Certificate and key pair not present");

        X509Certificate leafCertificate = certificateAndKeyPair.getCertificate();
        CheckNull.requireNonNull(leafCertificate,
                                 "Leaf certificate not present");
        CheckNull.requireNonNull(certificateAndKeyPair.getKey(),
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

            return new SecurityToken(securityToken, keyPairSupplier);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to get encoded x509 certificate", e);
        }
    }

    private String getSecurityTokenInternal(String publicKey,
                                            String leafCertificate,
                                            Set<String> interCerts) {

        String body = FederationRequestHelper.getFederationRequestBody(
            publicKey, leafCertificate, interCerts, purpose);
        logTrace(logger, "Federation request body " + body);
        return FederationRequestHelper.getSecurityToken(
            federationEndpoint, timeoutMS, tenantId,
            leafCertSupplier.getCertificateAndKeyPair(),
            body, logger);
    }

    static class SecurityToken {
        /* fields in security token JSON used to check validity */
        private static final String[] FIELDS = {
            "exp", "jwk", "n", "e",
            ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY,
            ResourcePrincipalClaimKeys.TENANT_ID_CLAIM_KEY};

        private final SessionKeyPairSupplier keyPairSupplier;
        private final String securityToken;
        private final Map<String, String> tokenClaims;

        SecurityToken(String token, SessionKeyPairSupplier supplier) {
            this.securityToken = token;
            this.keyPairSupplier = supplier;
            this.tokenClaims = parseToken(token);
        }

        String getSecurityToken() {
            return securityToken;
        }

        String getStringClaim(String key) {
            return tokenClaims.get(key);
        }

        /*
         * Return if the current token is still valid
         */
        boolean isValid(Logger logger) {
            if (tokenClaims == null) {
                logTrace(logger, "No security token cached");
                return false;
            }

            /* if token expired, return false */
            String exp = tokenClaims.get("exp");
            if (exp == null) {
                logTrace(logger, "Security token lack of expiration info");
                return false;
            }
            if ((Long.parseLong(exp) * 1000) < System.currentTimeMillis()) {
                return false;
            }

            /*
             * Next compare the public key inside the JWT is the same
             * from the supplier. We check this in case instance secrets
             * service deploys a new key and the JWT is still not expired.
             */
            String modulus = tokenClaims.get("n");
            String exponent = tokenClaims.get("e");
            if (modulus != null) {
                RSAPublicKey jwkRsa = Utils.toPublicKey(modulus, exponent);
                RSAPublicKey expected = (RSAPublicKey)
                    keyPairSupplier.getKeyPair().getPublic();
                if (jwkRsa != null &&
                    isEqualPublicKey(jwkRsa, expected)) {
                    logTrace(logger, "JWK in token is valid");
                    return true;
                }
            }
            return false;
        }

        /**
         * Checks if two public keys are equal
         * @param a one public key
         * @param b the other one
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

        /*
         * Parse security token JSON response get fields expiration time,
         * modulus and public exponent of JWK, only used for security token
         * validity check, ignores other fields.
         *
         * Response:
         * {
         *  "exp" : 1234123,
         *  "jwk" : {
         *    "e": "xxxx",
         *    "n": "xxxx",
         *    ...
         *  }
         *  ...
         * }
         */
        static Map<String, String> parseToken(String token) {
            if (token == null) {
                return null;
            }
            String[] jwt = splitJWT(token);
            String claimJson = new String(Base64.getUrlDecoder().decode(jwt[1]),
                                          Charset.forName("UTF-8"));

            try {
                JsonParser parser = createParser(claimJson);
                Map<String, String> results = new HashMap<>();
                parse(token, parser, results);

                String jwkString = results.get("jwk");
                if (jwkString == null) {
                    return results;
                }
                parser = createParser(jwkString);
                parse(token, parser, results);

                return results;
            } catch (IOException ioe) {
                throw new IllegalStateException(
                    "Error parsing security token "+ ioe.getMessage());
            }
        }

        private static void parse(String json,
                                  JsonParser parser,
                                  Map<String, String> reults)
            throws IOException {

            if (parser.getCurrentToken() == null) {
                parser.nextToken();
            }
            while (parser.getCurrentToken() != null) {
                String field = findField(json, parser, FIELDS);
                if (field != null) {
                    parser.nextToken();
                    reults.put(field, parser.getText());
                }
            }
        }

        private static String[] splitJWT(String jwt) {
            final int dot1 = jwt.indexOf(".");
            final int dot2 = jwt.indexOf(".", dot1 + 1);
            final int dot3 = jwt.indexOf(".", dot2 + 1);

            if (dot1 == -1 || dot2 == -1 || dot3 != -1) {
                throw new IllegalArgumentException(
                    "Given string is not in the valid access token format");
            }

            final String[] parts = new String[3];
            parts[0] = jwt.substring(0, dot1);
            parts[1] = jwt.substring(dot1 + 1, dot2);
            parts[2] = jwt.substring(dot2 + 1);
            return parts;
        }
    }
}
