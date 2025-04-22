/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Set;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.JDKKeyPairSupplier;

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

    protected final SecurityTokenSupplier tokenSupplier;
    protected final DefaultSessionKeySupplier sessionKeySupplier;
    private final Region region;

    /**
     * Constructor that accepts token and key pair suppliers and region
     * @param tokenSupplier the SecurityTokenSupplier
     * @param keyPairSupplier the SessionKeyPairSupplier
     * @param region the region
     */
    public InstancePrincipalsProvider(SecurityTokenSupplier tokenSupplier,
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

        /* The default purpose value in federation requests against IAM */
        private static final String DEFAULT_PURPOSE = "DEFAULT";

        private String purpose = DEFAULT_PURPOSE;
        private Logger logger;

        public String getFederationEndpoint() {
            return federationEndpoint;
        }

        public String getPurpose() {
            return purpose;
        }

        /**
         * @hidden
         * @param purpose the purpose
         * @return this
         */
        public InstancePrincipalsProviderBuilder setPurpose(String purpose) {
            this.purpose = purpose;
            return this;
        }

        public SessionKeyPairSupplier getSesssionKeyPairSupplier() {
            return sessSupplier;
        }

        /**
         * @hidden
         * @param sessSupplier the supplier
         * @return this
         */
        public InstancePrincipalsProviderBuilder
            setSessionKeyPairSupplier(SessionKeyPairSupplier sessSupplier) {
            this.sessSupplier = sessSupplier;
            return this;
        }

        public Logger getLogger() {
            return logger;
        }

        /**
         * @hidden
         * @param logger the logger
         * @return this
         */
        public InstancePrincipalsProviderBuilder setLogger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Set<CertificateSupplier> getIntermediateCertificateSuppliers() {
            return intermediateCertificateSuppliers;
        }

        /**
         * @hidden
         * @param suppliers suppliers
         * @return this
         */
        public InstancePrincipalsProviderBuilder
            setIntermediateCertificateSuppliers(
                Set<CertificateSupplier> suppliers) {
            this.intermediateCertificateSuppliers = suppliers;
            return this;
        }

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

            SessionKeyPairSupplier sessSupplierToUse = sessSupplier != null ? sessSupplier : new JDKKeyPairSupplier();

            SecurityTokenSupplier tokenSupplier =
                    new SecurityTokenSupplier(federationEndpoint,
                            tenantId,
                            leafCertificateSupplier,
                            intermediateCertificateSuppliers,
                            sessSupplierToUse,
                            purpose,
                            timeout,
                            logger);

            return new InstancePrincipalsProvider(tokenSupplier,
                    sessSupplierToUse,
                    region);
        }

    }
}
