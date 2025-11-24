package oracle.nosql.driver.iam;

import oracle.nosql.driver.NoSQLHandleConfig;

import java.util.logging.Logger;

/**
 * @hidden
 * Internal use only
 * <p>
 * The interface to supply security token for resource principal from fixed
 * String content.
 * Reference to the OCI SDK for Java
 * <code>com.oracle.bmc.auth.internal.FixedContentResourcePrincipalFederationClient</code>
 */
class FixedSecurityTokenSupplier
        implements TokenSupplier {

    private static final Logger logger = Logger.getLogger(FixedSecurityTokenSupplier.class.getName());

    private final SecurityTokenSupplier.SecurityToken securityToken;
    private long minTokenLifetime;

    FixedSecurityTokenSupplier(SessionKeyPairSupplier sessionKeySupplier,
                               String sessionToken) {
        this.securityToken = new SecurityTokenSupplier.SecurityToken(sessionToken,
                sessionKeySupplier);
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
}