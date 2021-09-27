/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.findField;
import static oracle.nosql.driver.iam.Utils.getIAMURL;
import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.util.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.iam.CertificateSupplier.DefaultCertificateSupplier;
import oracle.nosql.driver.iam.CertificateSupplier.URLResourceDetails;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.JDKKeyPairSupplier;
import oracle.nosql.driver.util.HttpRequestUtil;
import oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

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

    public InstancePrincipalsProvider(SecurityTokenSupplier tokenSupplier,
                                      SessionKeyPairSupplier keyPairSupplier,
                                      Region region) {
        this.tokenSupplier = tokenSupplier;
        this.sessionKeySupplier = new DefaultSessionKeySupplier(keyPairSupplier);
        this.region = region;
    }

    @Override
    public String getKeyId() {
        return "ST$" + tokenSupplier.getSecurityToken();
    }

    @Override
    public boolean isKeyValid(String keyId) {
        return keyId.equals("ST$" + tokenSupplier.getCurrentToken());
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
    public void setTokenExpirationRefreshWindow(long refreshWindowMS) {
        tokenSupplier.setTokenExpirationRefreshWindow(refreshWindowMS);
    }

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
        private static final JsonFactory factory = new JsonFactory();

        /* Instance metadata service base URL */
        private static final String METADATA_SERVICE_BASE_URL =
            "http://169.254.169.254/opc/v2/";
        private static final String FALLBACK_METADATA_SERVICE_URL =
            "http://169.254.169.254/opc/v1/";

        /* The authorization header need to send to metadata service since V2 */
        private static final String AUTHORIZATION_HEADER_VALUE = "Bearer Oracle";
        private static final String METADATA_SERVICE_HOST =
            "169.254.169.254";

        /* The default value for HTTP request timeouts in milliseconds */
        private static final int DEFAULT_TIMEOUT_MS = 120_000;

        /* The default purpose value in federation requests against IAM */
        private static final String DEFAULT_PURPOSE = "DEFAULT";

        /* Base metadata service URL */
        private String baseMetadataURL = METADATA_SERVICE_BASE_URL;

        /*
         * IAM federation endpoint, or null if decting from instance metadata.
         */
        private String federationEndpoint;

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

        public String getBaseMetadataURL() {
            return baseMetadataURL;
        }

        public String getFederationEndpoint() {
            return federationEndpoint;
        }

        public InstancePrincipalsProviderBuilder
            setFederationEndpoint(String federationEndpoint) {

            this.federationEndpoint = federationEndpoint;
            return this;
        }

        public CertificateSupplier getLeafCertificateSupplier() {
            return leafCertificateSupplier;
        }

        public InstancePrincipalsProviderBuilder
            setLeafCertificateSupplier(CertificateSupplier supplier) {

            this.leafCertificateSupplier = supplier;
            return this;
        }

        public String getTenantId() {
            return tenantId;
        }

        public InstancePrincipalsProviderBuilder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public String getPurpose() {
            return purpose;
        }

        public InstancePrincipalsProviderBuilder setPurpose(String purpose) {
            this.purpose = purpose;
            return this;
        }

        public SessionKeyPairSupplier getSesssionKeyPairSupplier() {
            return sessSupplier;
        }

        public InstancePrincipalsProviderBuilder
            setSessionKeyPairSupplier(SessionKeyPairSupplier sessSupplier) {
            this.sessSupplier = sessSupplier;
            return this;
        }

        public int getTimeout() {
            return timeout;
        }

        public InstancePrincipalsProviderBuilder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Logger getLogger() {
            return logger;
        }

        public InstancePrincipalsProviderBuilder setLogger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Set<CertificateSupplier> getIntermediateCertificateSuppliers() {
            return intermediateCertificateSuppliers;
        }

        public InstancePrincipalsProviderBuilder
            setIntermediateCertificateSuppliers(
                Set<CertificateSupplier> suppliers) {
            this.intermediateCertificateSuppliers = suppliers;
            return this;
        }

        public Region getRegion() {
            return region;
        }

        public InstancePrincipalsProviderBuilder setRegion(Region r) {
            this.region = r;
            return this;
        }

        public InstancePrincipalsProvider build() {
            if (logger == null) {
                logger = Logger.getLogger(getClass().getName());
                logger.setLevel(Level.WARNING);
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

            String instanceMDURL = getInstanceMetadaURL();
            logTrace(logger, "Detecting IAM endpoint using " + instanceMDURL);
            HttpClient client = null;
            try {
                client = new HttpClient(METADATA_SERVICE_HOST, 80,
                                        0, 0, 0, null, "InstanceMDClient",
                                        logger);
                HttpResponse response = HttpRequestUtil.doGetRequest
                    (client, instanceMDURL, headers(), timeout, logger);

                int status = response.getStatusCode();
                if (status == 404) {
                    logTrace(logger, "Falling back to v1 metadata URL, " +
                             "resource not found from v2");
                    this.baseMetadataURL = FALLBACK_METADATA_SERVICE_URL;
                    instanceMDURL = getInstanceMetadaURL();
                    response = HttpRequestUtil.doGetRequest
                        (client, instanceMDURL, headers(), timeout, logger);
                    if (response.getStatusCode() != 200) {
                        throw new IllegalStateException(
                            String.format("Unable to get federation URL from" +
                            "instance metadata " + METADATA_SERVICE_BASE_URL +
                            " or fallback to " + FALLBACK_METADATA_SERVICE_URL +
                            ", status code: %d, output: %s",
                            response.getOutput()));
                    }
                } else if (status != 200) {
                    throw new IllegalStateException(
                        String.format("Unable to get federation URL from" +
                        "instance metadata " + METADATA_SERVICE_BASE_URL +
                        ", status code: %d, output: %s",
                        response.getStatusCode(),
                        response.getOutput()));
                }

                logTrace(logger, "Instance metadata " + response.getOutput());
                String insRegion = findRegion(response.getOutput());
                logTrace(logger, "Instance region " + insRegion);

                federationEndpoint = getIAMURL(insRegion);
                if (federationEndpoint == null) {
                    throw new IllegalStateException(
                        "Invalid IAM URI, unknown region " + region);
                }
            } finally {
                if (client != null) {
                    client.shutdown();
                }
            }
        }

        private String getInstanceMetadaURL() {
            return getBaseMetadataURL() + "instance/";
        }

        private HttpHeaders headers() {
            return new DefaultHttpHeaders()
                .set(CONTENT_TYPE, APPLICATION_JSON)
                .set(AUTHORIZATION, AUTHORIZATION_HEADER_VALUE);
        }

        private String findRegion(String response) {
            try {
                JsonParser parser = factory.createParser(response);
                if (parser.getCurrentToken() == null) {
                    parser.nextToken();
                }
                while (parser.getCurrentToken() != null) {
                    String field = findField(response, parser, "region");
                    if (field != null) {
                        parser.nextToken();
                        return parser.getText();
                    }
                }
                throw new IllegalStateException(
                    "Unable to find region in instance metadata " + response);
            } catch (IOException ioe) {
                throw new IllegalStateException(
                    "Error parsing instance metadata in response " +
                    response+ " " + ioe.getMessage());
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
                        getURLDetails(getBaseMetadataURL() +
                                      "identity/cert.pem"),
                        getURLDetails(getBaseMetadataURL() +
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
                            getURLDetails(getBaseMetadataURL() +
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

            return new URLResourceDetails(new URL(url))
                    .addHeader(AUTHORIZATION, AUTHORIZATION_HEADER_VALUE);
        }
    }
}
