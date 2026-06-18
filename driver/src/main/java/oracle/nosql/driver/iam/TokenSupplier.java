package oracle.nosql.driver.iam;

import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * Defines a basic interface for token suppliers that provides a security
 * token for authentication.
 */
interface TokenSupplier {
    /**
     * Gets a security token from the federation endpoint.
     *
     * @return A security token that can be used to authenticate requests.
     */
    String getSecurityToken();

    /**
     * Builds HTTP client using config.
     */
    void prepare(NoSQLHandleConfig config);

    /**
     * Return the specific claim in security token by given key.
     */
    String getStringClaim(String key);

    /**
     * Set expected minimal token lifetime.
     */
    void setMinTokenLifetime(long lifetimeMS);

    /**
     * Cleanup the resources used by the token supplier
     */
    void close();
}
