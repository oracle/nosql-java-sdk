package oracle.nosql.driver.iam;

import oracle.nosql.driver.SecurityInfoNotReadyException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.logging.Logger;

import static oracle.nosql.driver.iam.Utils.logTrace;

/**
 * @hidden
 * Internal use only
 * <p>
 * This interface to supply security token for resource principal from file.
 * Reference to the OCI SDK for Java
 * <code>com.oracle.bmc.auth.internal.FileBasedResourcePrincipalFederationClient</code>
 */
public class FileSecurityTokenSupplier
        extends TokenSupplier {

    private static final Logger logger = Logger.getLogger(FileSecurityTokenSupplier.class.getName());

    private final SessionKeyPairSupplier sessionKeyPairSupplier;
    private final String sessionTokenPath;
    private volatile SecurityTokenSupplier.SecurityToken securityToken;

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

    private synchronized String refreshAndGetSecurityToken() {
        logTrace(logger, "Refreshing session keys");
        sessionKeyPairSupplier.refreshKeys();

        try {
            logTrace(logger, "Getting security token from file.");
            SecurityTokenSupplier.SecurityToken token = getSecurityTokenFromFile();
            token.validate(minTokenLifetime, logger);
            securityToken = token;
            return securityToken.getSecurityToken();
        } catch (Exception e) {
            throw new SecurityInfoNotReadyException(e.getMessage(), e);
        }
    }

    SecurityTokenSupplier.SecurityToken getSecurityTokenFromFile() {
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

        return new SecurityTokenSupplier.SecurityToken(sessToken, sessionKeyPairSupplier);
    }
}
