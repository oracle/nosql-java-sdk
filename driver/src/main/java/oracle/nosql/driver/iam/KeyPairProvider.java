package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;

/**
 * Resource Principals V2 using public/private key to sign the request. This class provides the
 * authentication based on public/private key.
 */
public class KeyPairProvider
        implements AuthenticationProfileProvider,
                   SecurityTokenBasedProvider {

    private final String resourceId;
    private final InputStream privateKeyStream;
    private final char[] passphrase;
    private final String tenancyId;
    private final String resourcePrincipalVersion;

    /**
     * Constructor of KeyPairProvider
     *
     * @param resourceId resource id of the resource
     * @param privateKeyStream private key stream to sign the request
     * @param passphrase passphrase for the private key
     * @param tenancyId tenancy id of the resource
     * @param resourcePrincipalVersion resource principal version
     */
    public KeyPairProvider(
            final String resourceId,
            final InputStream privateKeyStream,
            final char[] passphrase,
            final String tenancyId,
            final String resourcePrincipalVersion) {
        this.resourceId = resourceId;
        this.privateKeyStream = privateKeyStream;
        this.passphrase = passphrase;
        this.tenancyId = tenancyId;
        this.resourcePrincipalVersion = resourcePrincipalVersion;
    }

    /**
     * Returns the keyId used to sign requests.
     *
     * @return The keyId.
     */
    @Override
    public String getKeyId() {
        if (tenancyId != null) {
            if (Utils.isNotBlank(resourcePrincipalVersion)
                    && resourcePrincipalVersion.equals("2.1.1")) {
                return "resource/v2.1.1/" + this.tenancyId + "/" + this.resourceId;
            }
            if (Utils.isNotBlank(resourcePrincipalVersion)
                    && resourcePrincipalVersion.equals("2.1.2")) {
                return "resource/v2.1.2/" + this.tenancyId + "/" + this.resourceId;
            }
        }
        return "resource/v2.1/" + this.resourceId;
    }

    /**
     * Returns a new InputStream to the private key. This stream should be closed by the caller,
     * implementations should return new streams each time.
     *
     * @return A new InputStream.
     */
    @Override
    public InputStream getPrivateKey() {
        try {
            return new ByteArrayInputStream(Utils.toByteArray(this.privateKeyStream));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read private key stream.", ex);
        }
    }

    /**
     * Returns the optional pass phrase for the (encrypted) private key, as a character array.
     *
     * @return The pass phrase as character array, or null if not applicable
     */
    @Override
    public char[] getPassphraseCharacters() {
        return this.passphrase;
    }

    /**
     * Returns the Resource ID of the resource
     *
     * @return the resourceId
     */
    public String getResourceId() {
        return resourceId;
    }

    @Override
    public void setMinTokenLifetime(long lifetimeMS) {}

}
