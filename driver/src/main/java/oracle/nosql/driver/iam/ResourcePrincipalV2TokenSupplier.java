/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;
import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.iam.Utils.parseResourcePrincipalTokenResponse;
import static oracle.nosql.driver.iam.Utils.base64EncodeNoChunking;

import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;
import java.util.logging.Logger;

import oracle.nosql.driver.util.HttpRequestUtil;

class ResourcePrincipalV2TokenSupplier extends AbstractTokenSupplier {
    private static final Logger logger =
            Logger.getLogger(ResourcePrincipalV2TokenSupplier.class.getName());
    private final KeyPairProvider keyPairprovider;
    private final String securityContext;

    public ResourcePrincipalV2TokenSupplier(
            String resourcePrincipalTokenEndpoint,
            String federationEndpoint,
            String resourcePrincipalTokenPath,
            SessionKeyPairSupplier sessionKeySupplier,
            KeyPairProvider provider,
            String securityContext
    ) {
        super(
                resourcePrincipalTokenEndpoint,
                federationEndpoint,
                resourcePrincipalTokenPath,
                sessionKeySupplier,
                logger
        );

        this.keyPairprovider = provider;
        this.securityContext = securityContext;
    }

    @Override
    public void close() {
        super.close();
        if (keyPairprovider != null) {
            keyPairprovider.close();
        }
    }

    @Override
    protected SecurityToken getSecurityTokenFromServer() {
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

        // get keyId and private key from keyPairSupplier
        keyId = keyPairprovider.getKeyId();
        privateKeyProvider = new PrivateKeyProvider(keyPairprovider);

        HttpRequestUtil.HttpResponse resourcePrincipalTokenResponse =
                getResourcePrincipalTokenResponse(securityContext);

        Map<String, String> resourcePrincipalTokenOutput =
                parseResourcePrincipalTokenResponse(
                        resourcePrincipalTokenResponse.getOutput());

        String resourcePrincipalToken =
                resourcePrincipalTokenOutput.get("resourcePrincipalToken");
        String servicePrincipalSessionToken =
                resourcePrincipalTokenOutput.get("servicePrincipalSessionToken");

        return getResourcePrincipalSessionToken(
                resourcePrincipalToken,
                servicePrincipalSessionToken,
                base64EncodeNoChunking(publicKey)
        );
    }
}
