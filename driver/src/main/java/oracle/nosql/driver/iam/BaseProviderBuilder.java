/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import oracle.nosql.driver.Region;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import static oracle.nosql.driver.iam.Utils.getIAMURL;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

public class BaseProviderBuilder<T extends BaseProviderBuilder<T>> {
    /* The default value for HTTP request timeouts in milliseconds */
    private static final int DEFAULT_TIMEOUT_MS = 5_000;

    /*
     * IAM federation endpoint, or null if detecting from instance metadata.
     */
    protected String federationEndpoint = null;

    /*
     * The leaf certificate, or null if detecting from instance metadata.
     */
    protected CertificateSupplier leafCertificateSupplier;

    /*
     * Intermediate certificates or null if detecting from instance metadata.
     */
    protected Set<CertificateSupplier> intermediateCertificateSuppliers;

    /* Instance metadata */
    private InstanceMetadataHelper.InstanceMetadata instanceMetadata;

    /*
     * Session key pair supplier.
     */
    protected SessionKeyPairSupplier sessSupplier = null;

    protected String tenantId;
    protected int timeout = DEFAULT_TIMEOUT_MS;
    protected Region region;
    private Logger logger;

    public String getFederationEndpoint() {
        return federationEndpoint;
    }

    /**
     * @hidden
     * @param federationEndpoint the endpoint
     * @return this
     */
    public T setFederationEndpoint(String federationEndpoint) {
        this.federationEndpoint = federationEndpoint;
        return (T) this;
    }

    public CertificateSupplier getLeafCertificateSupplier() {
        return leafCertificateSupplier;
    }

    /**
     * @hidden
     * @param supplier the supplier
     * @return this
     */
    public T setLeafCertificateSupplier(CertificateSupplier supplier) {
        this.leafCertificateSupplier = supplier;
        return (T) this;
    }

    public String getTenantId() {
        return tenantId;
    }

    /**
     * @hidden
     * @param tenantId the tenant
     * @return this
     */
    public T setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return (T) this;
    }

    public SessionKeyPairSupplier getSesssionKeyPairSupplier() {
        return sessSupplier;
    }

    /**
     * @hidden
     * @param sessSupplier the supplier
     * @return this
     */
    public T setSessionKeyPairSupplier(SessionKeyPairSupplier sessSupplier) {
        this.sessSupplier = sessSupplier;
        return (T) this;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * @hidden
     * @param timeout the timeout
     * @return this
     */
    public T setTimeout(int timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * @hidden
     * @param logger the logger
     * @return this
     */
    public T setLogger(Logger logger) {
        this.logger = logger;
        return (T) this;
    }

    public Set<CertificateSupplier> getIntermediateCertificateSuppliers() {
        return intermediateCertificateSuppliers;
    }

    /**
     * @hidden
     * @param suppliers suppliers
     * @return this
     */
    public T setIntermediateCertificateSuppliers(
            Set<CertificateSupplier> suppliers) {
        this.intermediateCertificateSuppliers = suppliers;
        return (T) this;
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
    public T setRegion(Region r) {
        this.region = r;
        return (T) this;
    }

    /*
     * Auto detects the endpoint and region that should be used
     * when talking to IAM, if no endpoint has been configured already.
     */
    protected void autoDetectEndpointUsingMetadataUrl() {
        final String insRegion = getInstanceMetadata().getRegion();
        if(federationEndpoint == null) {
            federationEndpoint = getIAMURL(insRegion);
        }
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
    protected void autoDetectCertificatesUsingMetadataUrl() {
        try {
            if (leafCertificateSupplier == null) {
                leafCertificateSupplier = new CertificateSupplier.DefaultCertificateSupplier(
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
                        new CertificateSupplier.DefaultCertificateSupplier(
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

    private CertificateSupplier.URLResourceDetails getURLDetails(String url)
            throws MalformedURLException {

        return new CertificateSupplier.URLResourceDetails(new URL(url)).addHeader(
                AUTHORIZATION,
                InstanceMetadataHelper.AUTHORIZATION_HEADER_VALUE);
    }

    private InstanceMetadataHelper.InstanceMetadata getInstanceMetadata() {
        if (instanceMetadata == null) {
            instanceMetadata = InstanceMetadataHelper
                    .fetchMetadata(timeout, logger);
        }
        return instanceMetadata;
    }
}
