package oracle.nosql.driver.iam;

/**
 * Defines a basic abstract class for a resource principal token supplier that provides
 * a security token for authentication.
 */
public abstract class TokenSupplier {

    /*
     * The expected minimal lifetime is configured by SignatureProvider,
     * the same as the signature cache duration. Security token providers
     * should validate token to ensure it has the expected minimal lifetime,
     * throw an error otherwise.
     */
    long minTokenLifetime;

    /**
     * Gets a security token from the federation endpoint.
     *
     * @return A security token that can be used to authenticate requests.
     */
    abstract String getSecurityToken();

    /**
     * Get a claim embedded in the security token.
     */
    abstract String getStringClaim(String key);

    /**
     * Set expected minimal token lifetime.
     */
    void setMinTokenLifetime(long lifetimeMS) {
        minTokenLifetime = lifetimeMS;
    }

    /**
     * Cleanup the resources used by the token supplier
     */
    abstract void close();
}
