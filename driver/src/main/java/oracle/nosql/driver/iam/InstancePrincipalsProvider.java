/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;

/**
 * Internal use only
 * <p>
 * The authentication profile provider used to call service API from OCI
 * compute instance. It authenticates with instance principal and uses security
 * token issued by IAM to do the actual request signing.
 * @hidden
 */
public class InstancePrincipalsProvider
    implements AuthenticationProfileProvider,
               RegionProvider,
               SecurityTokenBasedProvider {

    protected final TokenSupplier tokenSupplier;
    protected final DefaultSessionKeySupplier sessionKeySupplier;
    private final Region region;

    /**
     * Constructor that accepts token and key pair suppliers and region
     * @param tokenSupplier the SecurityTokenSupplier
     * @param keyPairSupplier the SessionKeyPairSupplier
     * @param region the region
     */
    public InstancePrincipalsProvider(TokenSupplier tokenSupplier,
                                      SessionKeyPairSupplier keyPairSupplier,
                                      Region region) {
        this.tokenSupplier = tokenSupplier;
        this.sessionKeySupplier = new DefaultSessionKeySupplier(keyPairSupplier);
        this.region = region;
    }

    /**
     * @hidden
     * @param config the config
     */
    public void prepare(NoSQLHandleConfig config) {
        tokenSupplier.prepare(config);
    }

    public void close() {
        tokenSupplier.close();
    }

    @Override
    public String getKeyId() {
        return "ST$" + tokenSupplier.getSecurityToken();
    }


    @Override
    public InputStream getPrivateKey() {
        return new ByteArrayInputStream(sessionKeySupplier.getPrivateKeyBytes());
    }

    @Override
    public char[] getPassphraseCharacters() {
        return null;
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public void setMinTokenLifetime(long lifetimeMS) {
        tokenSupplier.setMinTokenLifetime(lifetimeMS);
    }

    /**
     * Creates a new builder for InstancePrincipalsProvider
     * @return the builder
     */
    public static InstancePrincipalsProviderBuilder builder() {
        return new InstancePrincipalsProviderBuilder();
    }

    /**
     * Cloud service only.
     * <p>
     * Builder of InstancePrincipalsProvider
     * @hidden
     */
    public static class InstancePrincipalsProviderBuilder extends BaseProviderBuilder<InstancePrincipalsProviderBuilder> {
        /**
         * Builds the InstancePrincipalsProvider instance
         * @return the instance
         */
        public InstancePrincipalsProvider build() {
            if (logger == null) {
                logger = Logger.getLogger(getClass().getName());
            }
            autoDetectEndpointUsingMetadataUrl();
            autoDetectCertificatesUsingMetadataUrl();

            TokenSupplier tokenSupplier =
                    new SecurityTokenSupplier(federationEndpoint,
                            tenantId,
                            leafCertificateSupplier,
                            intermediateCertificateSuppliers,
                            sessionKeySupplier,
                            purpose,
                            timeout,
                            logger);

            return new InstancePrincipalsProvider(tokenSupplier,
                    sessionKeySupplier,
                    region);
        }

    }
}
