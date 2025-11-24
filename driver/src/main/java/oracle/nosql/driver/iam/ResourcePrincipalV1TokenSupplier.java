/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.iam.Utils.parseResourcePrincipalTokenResponse;
import static oracle.nosql.driver.iam.Utils.base64EncodeNoChunking;
import static oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;
import static oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;

import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;
import java.util.logging.Logger;

class ResourcePrincipalV1TokenSupplier extends AbstractTokenSupplier {
    private static final Logger logger =
            Logger.getLogger(ResourcePrincipalV1TokenSupplier.class.getName());
    private final InstancePrincipalsProvider instancePrincipalProvider;

    public ResourcePrincipalV1TokenSupplier(
            String resourcePrincipalTokenEndpoint,
            String federationEndpoint,
            String resourcePrincipalTokenPath,
            SessionKeyPairSupplier sessionKeySupplier,
            InstancePrincipalsProvider provider
    ) {
        super(
                resourcePrincipalTokenEndpoint,
                federationEndpoint,
                resourcePrincipalTokenPath,
                sessionKeySupplier,
                logger
        );
        this.instancePrincipalProvider = provider;
    }

    @Override
    public void close() {
        super.close();
        if (instancePrincipalProvider != null) {
            instancePrincipalProvider.close();
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

        // Refresh and get instance principal token with Identity
        keyId = instancePrincipalProvider.getKeyId();
        privateKeyProvider = new PrivateKeyProvider(instancePrincipalProvider);

        HttpResponse resourcePrincipalTokenResponse =
                getResourcePrincipalTokenResponse(null);

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
