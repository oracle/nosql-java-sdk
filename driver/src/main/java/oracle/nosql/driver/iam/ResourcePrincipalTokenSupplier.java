/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.logging.Logger;

import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;

/**
 * @hidden
 * Internal use only
 * <p>
 * The class to supply security token for resource principal
 */
abstract class ResourcePrincipalTokenSupplier {
    /* Refresh window before token is expired */
    protected long tokenExpirationRefreshWindow;

    /**
     * Return the security token of resource principal.
     */
    abstract String getSecurityToken();

    /**
     * Return the specific claim in security token by given key.
     */
    abstract String getStringClaim(String key);

    /**
     * Return current cached token.
     */
    abstract String getCurrentToken();

    /**
     * Set token expiration refresh window.
     */
    void setTokenExpirationRefreshWindow(long refreshWindowMS) {
        this.tokenExpirationRefreshWindow = refreshWindowMS;
    }

    /**
     * @hidden
     * Internal use only
     * <p>
     * This interface to supply security token for resource principal from file.
     * Reference to the OCI SDK for Java
     * <code>com.oracle.bmc.auth.internal.FileBasedResourcePrincipalFederationClient</code>
     */
    static class FileSecurityTokenSupplier
        extends ResourcePrincipalTokenSupplier {

        private final SessionKeyPairSupplier sessionKeyPairSupplier;
        private final String sessionTokenPath;
        private volatile SecurityToken securityToken;
        private Logger logger;

        FileSecurityTokenSupplier(SessionKeyPairSupplier sessKeyPairSupplier,
                                  String sessionTokenPath,
                                  Logger logger) {
            this.sessionKeyPairSupplier = sessKeyPairSupplier;
            this.sessionTokenPath = sessionTokenPath;
            this.securityToken = new SecurityToken(null,
                                                   tokenExpirationRefreshWindow,
                                                   sessionKeyPairSupplier);
            this.logger = logger;
        }

        @Override
        public String getSecurityToken() {
            if (securityToken.isValid(logger)) {
                return securityToken.getSecurityToken();
            }
            return refreshAndGetSecurityToken();
        }

        @Override
        public String getStringClaim(String key) {
            refreshAndGetSecurityToken();
            return securityToken.getStringClaim(key);
        }

        @Override
        public String getCurrentToken() {
            if (securityToken.isValid(logger, false)) {
                return securityToken.getSecurityToken();
            }
            return null;
        }

        private String refreshAndGetSecurityToken() {
            synchronized (this) {
                if (!securityToken.isValid(logger)) {
                    logTrace(logger, "Refreshing session keys");
                    sessionKeyPairSupplier.refreshKeys();

                    logTrace(logger, "Getting security token from file.");
                    securityToken = getSecurityTokenFromFile();
                    return securityToken.getSecurityToken();
                }

                return securityToken.getSecurityToken();
            }
        }

        SecurityToken getSecurityTokenFromFile() {
            KeyPair keyPair = sessionKeyPairSupplier.getKeyPair();
            if (keyPair == null) {
                throw new IllegalArgumentException(
                    "Keypair for session was not provided");
            }

            String sessToken = null;
            try {
                sessToken = new String(
                    Files.readAllBytes(Paths.get(sessionTokenPath)),
                    Charset.defaultCharset());
            } catch (IOException e) {
                throw new RuntimeException(
                    "Unable to read session token from " + sessionTokenPath, e);
            }

            return new SecurityToken(sessToken,
                                     tokenExpirationRefreshWindow,
                                     sessionKeyPairSupplier);
        }
    }

    /**
     * @hidden
     * Internal use only
     * <p>
     * This interface to supply security token for resource principal from fixed
     * String content.
     * Reference to the OCI SDK for Java
     * <code>com.oracle.bmc.auth.internal.FixedContentResourcePrincipalFederationClient</code>
     */
    static class FixedSecurityTokenSupplier
        extends ResourcePrincipalTokenSupplier {

        private final SecurityToken securityToken;

        FixedSecurityTokenSupplier(SessionKeyPairSupplier sessionKeySupplier,
                                   String sessionToken) {
            this.securityToken = new SecurityToken(sessionToken,
                                                   tokenExpirationRefreshWindow,
                                                   sessionKeySupplier);
        }

        @Override
        public String getSecurityToken() {
            return securityToken.getSecurityToken();
        }

        @Override
        public String getStringClaim(String key) {
            return securityToken.getStringClaim(key);
        }

        @Override
        public String getCurrentToken() {
            return securityToken.getSecurityToken();
        }
    }
}
