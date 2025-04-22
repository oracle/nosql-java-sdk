package oracle.nosql.driver.iam;

import java.util.logging.Logger;

/**
 * @hidden
 * Internal use only
 * <p>
 * This interface to supply security token for resource principal from fixed
 * String content.
 * Reference to the OCI SDK for Java
 * <code>com.oracle.bmc.auth.internal.FixedContentResourcePrincipalFederationClient</code>
 */
public class FixedSecurityTokenSupplier
        extends TokenSupplier {

    private static final Logger logger = Logger.getLogger(FixedSecurityTokenSupplier.class.getName());

    private final SecurityTokenSupplier.SecurityToken securityToken;

    FixedSecurityTokenSupplier(SessionKeyPairSupplier sessionKeySupplier,
                               String sessionToken) {
        this.securityToken = new SecurityTokenSupplier.SecurityToken(sessionToken,
                sessionKeySupplier);
    }

    @Override
    public void close() {

    }

    @Override
    public String getSecurityToken() {
        securityToken.validate(minTokenLifetime, logger);
        return securityToken.getSecurityToken();
    }

    @Override
    public String getStringClaim(String key) {
        return securityToken.getStringClaim(key);
    }
}