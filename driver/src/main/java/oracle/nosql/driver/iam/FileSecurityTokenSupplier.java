/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.SecurityInfoNotReadyException;

/**
 * @hidden
 * Internal use only
 * <p>
 * The interface to supply security token for resource principal from file.
 * Reference to the OCI SDK for Java
 * <code>com.oracle.bmc.auth.internal.FileBasedResourcePrincipalFederationClient</code>
 */
class FileSecurityTokenSupplier
        implements TokenSupplier {

    private static final Logger logger = Logger.getLogger(FileSecurityTokenSupplier.class.getName());

    private final SessionKeyPairSupplier sessionKeyPairSupplier;
    private final String sessionTokenPath;
    private volatile SecurityTokenSupplier.SecurityToken securityToken;
    private long minTokenLifetime;

    FileSecurityTokenSupplier(SessionKeyPairSupplier sessKeyPairSupplier,
                              String sessionTokenPath) {
        this.sessionKeyPairSupplier = sessKeyPairSupplier;
        this.sessionTokenPath = sessionTokenPath;
    }

    @Override
    public String getSecurityToken() {
        return refreshAndGetSecurityToken();
    }

    @Override
    public String getStringClaim(String key) {
        if (securityToken == null) {
            refreshAndGetSecurityToken();
        }
        return securityToken.getStringClaim(key);
    }

    @Override
    public void setMinTokenLifetime(long lifetimeMS) {
        this.minTokenLifetime = lifetimeMS;
    }

    @Override
    public void close() {
    }

    @Override
    public void prepare(NoSQLHandleConfig config) {
    }

    private synchronized String refreshAndGetSecurityToken() {
        logTrace(logger, "Refreshing session keys");
        sessionKeyPairSupplier.refreshKeys();

        try {
            logTrace(logger, "Getting security token from file.");
            SecurityToken token = getSecurityTokenFromFile();
            token.validate(minTokenLifetime, logger);
            securityToken = token;
            return securityToken.getSecurityToken();
        } catch (Exception e) {
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
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

        return new SecurityToken(sessToken, sessionKeyPairSupplier);
    }
}
