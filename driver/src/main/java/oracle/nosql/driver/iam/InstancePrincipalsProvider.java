/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.getIAMURL;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.CertificateSupplier.DefaultCertificateSupplier;
import oracle.nosql.driver.iam.CertificateSupplier.URLResourceDetails;
import oracle.nosql.driver.iam.InstanceMetadataHelper.InstanceMetadata;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.JDKKeyPairSupplier;

/**
 * @hidden
 * Internal use only
 * <p>
 * The authentication profile provider used to call service API from OCI
 * compute instance. It authenticates with instance principal and uses security
 * token issued by IAM to do the actual request signing.
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
     * @hidden
     * Cloud service only.
     * <p>
     * Builder of InstancePrincipalsProvider
     */
    public static class InstancePrincipalsProviderBuilder {
        /* The default value for HTTP request timeouts in milliseconds */
        private static final int DEFAULT_TIMEOUT_MS = 5_000;

        /* The default purpose value in federation requests against IAM */
        private static final String DEFAULT_PURPOSE = "DEFAULT";

        /*
         * IAM federation endpoint, or null if detecting from instance metadata.
         */
        private String federationEndpoint;

        /* Instance metadata */
        private InstanceMetadata instanceMetadata;

        /*
         * The leaf certificate, or null if detecting from instance metadata.
         */
        private CertificateSupplier leafCertificateSupplier;

        /*
         * Intermediate certificates or null if detecting from instance metadata.
         */
        private Set<CertificateSupplier> intermediateCertificateSuppliers;

        /*
         * Session key pair supplier.
         */
        private SessionKeyPairSupplier sessSupplier = new JDKKeyPairSupplier();

        /*
         * Tenant id, or null if detecting from instance metadata.
         */
        private String tenantId;
        private String purpose = DEFAULT_PURPOSE;
        private int timeout = DEFAULT_TIMEOUT_MS;
        private Region region;
        private Logger logger;

        public String getFederationEndpoint() {
            return federationEndpoint;
        }

        /**
         * @hidden
         * @param federationEndpoint the endpoint
         * @return this
         */
        public InstancePrincipalsProviderBuilder
            setFederationEndpoint(String federationEndpoint) {

            this.federationEndpoint = federationEndpoint;
            return this;
        }

        public CertificateSupplier getLeafCertificateSupplier() {
            return leafCertificateSupplier;
        }

        /**
         * @hidden
         * @param supplier the supplier
         * @return this
         */
        public InstancePrincipalsProviderBuilder
            setLeafCertificateSupplier(CertificateSupplier supplier) {

            this.leafCertificateSupplier = supplier;
            return this;
        }

        public String getTenantId() {
            return tenantId;
        }

        /**
         * @hidden
         * @param tenantId the tenant
         * @return this
         */
        public InstancePrincipalsProviderBuilder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
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

        public int getTimeout() {
            return timeout;
        }

        /**
         * @hidden
         * @param timeout the timeout
         * @return this
         */
        public InstancePrincipalsProviderBuilder setTimeout(int timeout) {
            this.timeout = timeout;
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
         * Returns the region if set
         * @return the region or null if not set
         */
        public Region getRegion() {
            return region;
        }

        /**
         * Sets a region
         * @param r the region
         * @return this
         */
        public InstancePrincipalsProviderBuilder setRegion(Region r) {
            this.region = r;
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

            SecurityTokenSupplier tokenSupplier =
                new SecurityTokenSupplier(federationEndpoint,
                                          tenantId,
                                          leafCertificateSupplier,
                                          intermediateCertificateSuppliers,
                                          sessSupplier,
                                          purpose,
                                          timeout,
                                          logger);

            return new InstancePrincipalsProvider(tokenSupplier,
                                                  sessSupplier,
                                                  region);
        }

        /*
         * Auto detects the endpoint that should be used when talking to
         * IAM, if no endpoint has been configured already.
         */
        private void autoDetectEndpointUsingMetadataUrl() {
            if (federationEndpoint != null) {
                return;
            }

            final String insRegion = getInstanceMetadata().getRegion();
            federationEndpoint = getIAMURL(insRegion);
            if (region == null) {
                region = Region.fromRegionId(insRegion);
            }
            if (federationEndpoint == null) {
                throw new IllegalArgumentException(
                    "Unable to find IAM URL for unregistered region " +
                    region + ", specify the IAM URL instead");
            }
        }

        /*
         * Auto detects and configures the certificates needed
         * using Instance metadata.
         */
        private void autoDetectCertificatesUsingMetadataUrl() {
            try {
                if (leafCertificateSupplier == null) {
                    leafCertificateSupplier = new DefaultCertificateSupplier(
                        getURLDetails(getInstanceMetadata().getBaseURL() +
                                      "identity/cert.pem"),
                        getURLDetails(getInstanceMetadata().getBaseURL() +
                                      "identity/key.pem"),
                        (char[]) null);
                }

                if (tenantId == null) {
                    tenantId = Utils.getTenantId(leafCertificateSupplier
                        .getCertificateAndKeyPair().getCertificate());
                }

                if (intermediateCertificateSuppliers == null) {
                    intermediateCertificateSuppliers = new HashSet<>();

                    intermediateCertificateSuppliers.add(
                        new DefaultCertificateSupplier(
                            getURLDetails(getInstanceMetadata().getBaseURL() +
                                          "identity/intermediate.pem"),
                            null,
                            (char[]) null));
                }
            } catch (MalformedURLException ex) {
                throw new IllegalArgumentException(
                    "The instance metadata service url is invalid.", ex);
            }
        }

        private URLResourceDetails getURLDetails(String url)
            throws MalformedURLException {

            return new URLResourceDetails(new URL(url)).addHeader(
                AUTHORIZATION,
                InstanceMetadataHelper.AUTHORIZATION_HEADER_VALUE);
        }

        private InstanceMetadata getInstanceMetadata() {
            if (instanceMetadata == null) {
                instanceMetadata = InstanceMetadataHelper
                    .fetchMetadata(timeout, logger);
            }
            return instanceMetadata;
        }
    }
}
