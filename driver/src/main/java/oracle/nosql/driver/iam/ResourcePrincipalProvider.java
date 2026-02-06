/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.isBlank;
import static oracle.nosql.driver.iam.Utils.isNotBlank;
import static oracle.nosql.driver.iam.Utils.logTrace;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.logging.Logger;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FileKeyPairSupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FixedKeyPairSupplier;
import oracle.nosql.driver.NoSQLHandleConfig;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @hidden
 * Internal use only
 * <p>
 * The authentication profile provider used to call service API from other OCI
 * resource such as function. It authenticates with resource principal and uses
 * security token issued by IAM to do the actual request signing.
 * <p>
 * It's constructed in accordance with the following environment variables:
 *  <ul>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_VERSION}: permitted values are "2.2"
 * </li>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_RPST}:
 * <p>
 * If this is an absolute path, then the filesystem-supplied resource
 * principal session token will be retrieved from that location. This mode
 * supports token refresh (if the environment replaces the RPST in the
 * filesystem). Otherwise, the environment variable is taken to hold the raw
 * value of an RPST. Under these circumstances, the RPST cannot be refreshed;
 * consequently, this mode is only usable for short-lived executables.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM}:
 * If this is an absolute path, then the filesystem-supplied private key will
 * be retrieved from that location. As with the OCI_RESOURCE_PRINCIPAL_RPST,
 * this mode supports token refresh if the environment can update the file
 * contents. Otherwise, the value is interpreted as the direct injection of a
 * private key. The same considerations as to the lifetime of this value apply
 * when directly injecting a key.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE}:
 * <p>
 * This is optional. If set, it contains either the location (as an absolute
 * path) or the value of the passphrase associated with the private key.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_REGION}:
 * <p>
 * If set, this holds the canonical form of the local region. This is intended
 * to enable executables to locate their "local" OCI service endpoints.</p>
 * </li>
 * </ul>
 */
public class ResourcePrincipalProvider
    implements AuthenticationProfileProvider,
               RegionProvider,
               SecurityTokenBasedProvider {
    private static final Logger logger = Logger.getLogger(ResourcePrincipalProvider.class.getName());

    /* Environment variable names used to fetch artifacts */
    private static final String OCI_RESOURCE_PRINCIPAL_VERSION =
            "OCI_RESOURCE_PRINCIPAL_VERSION";
    protected static final String RP_VERSION_1_1 = "1.1";
    protected static final String RP_VERSION_2_1 = "2.1";
    protected static final String RP_VERSION_2_1_1 = "2.1.1";
    protected static final String RP_VERSION_2_1_2 = "2.1.2";
    protected static final String RP_VERSION_2_2 = "2.2";
    protected static final String RP_VERSION_3_0 = "3.0";
    protected static final String OCI_RESOURCE_PRINCIPAL_RPST =
            "OCI_RESOURCE_PRINCIPAL_RPST";
    protected static final String OCI_RESOURCE_PRINCIPAL_REGION =
            "OCI_RESOURCE_PRINCIPAL_REGION";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM =
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE =
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT =
            "OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT";
    private static final String OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT =
            "OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT";
    private static final String OCI_RESOURCE_PRINCIPAL_RPT_PATH = "OCI_RESOURCE_PRINCIPAL_RPT_PATH";
    static final String OCI_RESOURCE_PRINCIPAL_REGION_ENV_VAR_NAME =
            "OCI_RESOURCE_PRINCIPAL_REGION";
    private static final String OCI_RESOURCE_PRINCIPAL_RESOURCE_ID =
            "OCI_RESOURCE_PRINCIPAL_RESOURCE_ID";
    private static final String OCI_RESOURCE_PRINCIPAL_TENANCY_ID =
            "OCI_RESOURCE_PRINCIPAL_TENANCY_ID";
    private static final String OCI_RESOURCE_PRINCIPAL_SECURITY_CONTEXT =
            "OCI_RESOURCE_PRINCIPAL_SECURITY_CONTEXT";

    private static final String DEFAULT_OCI_RESOURCE_PRINCIPAL_RPT_PATH_FORV2_1_OR_2_1_1 =
            "20180711/resourcePrincipalTokenV2";
    private static final String DEFAULT_OCI_RESOURCE_PRINCIPAL_RPT_PATH_FORV212 =
            "20180711/resourcePrincipalTokenV212";

    private static final String RP_DEBUG_INFORMATION_LOG =
            "\nResource principals authentication can only be used in certain OCI services. Please check that the OCI service you're running this code from supports Resource principals."
                    + "\nSee https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_resource_principal for more info.";

    private final TokenSupplier tokenSupplier;
    private final DefaultSessionKeySupplier sessionKeySupplier;
    private final Region region;

    /**
     * Constructor of ResourcePrincipalAuthenticationDetailsProvider.
     *
     * @param tokenSupplier token supplier implementation.
     * @param sessionKeyPairSupplier session key supplier implementation.
     * @param region the region
     */
    ResourcePrincipalProvider(TokenSupplier tokenSupplier,
                              SessionKeyPairSupplier sessionKeyPairSupplier,
                              Region region) {
        this.tokenSupplier = tokenSupplier;
        this.sessionKeySupplier = new DefaultSessionKeySupplier(sessionKeyPairSupplier);
        this.region = region;
    }

    @Override
    public void prepare(NoSQLHandleConfig config) {
        tokenSupplier.prepare(config);
    }

    @Override
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

    public String getClaim(String key) {
        return tokenSupplier.getStringClaim(key);
    }

    public static ResourcePrincipalProviderBuilder builder(){
        return new ResourcePrincipalProviderBuilder();
    }

    /**
     * Cloud service only.
     * <p>
     * Builder of ResourcePrincipalProvider
     * @hidden
     */
    public static class ResourcePrincipalProviderBuilder extends BaseProviderBuilder<ResourcePrincipalProviderBuilder> {

        ResourcePrincipalProviderBuilder() {}

        /** The endpoint that can provide the resource principal token. */
        protected String resourcePrincipalTokenEndpoint;

        /**  The path provider for the resource principal token. */
        protected RptPathProvider resourcePrincipalTokenPathProvider;

        /** The configuration for the security context. */
        protected String securityContext;

        /** Configures the resourcePrincipalTokenPathProvider to use. */
        public ResourcePrincipalProviderBuilder setResourcePrincipalTokenPathProvider(
                RptPathProvider resourcePrincipalTokenPathProvider
        ) {
            this.resourcePrincipalTokenPathProvider = resourcePrincipalTokenPathProvider;
            return this;
        }

        /** Configures the resourcePrincipalTokenEndpoint to use. */
        public ResourcePrincipalProviderBuilder setResourcePrincipalTokenEndpoint(
                String resourcePrincipalTokenEndpoint
        ) {
            this.resourcePrincipalTokenEndpoint = resourcePrincipalTokenEndpoint;
            return this;
        }

        /** Set value for the security context to use. */
        public ResourcePrincipalProviderBuilder setSecurityContext(String securityContext) {
            this.securityContext = securityContext;
            return this;
        }

        public ResourcePrincipalProvider build() {
            if (logger == null) {
                logger = Logger.getLogger(getClass().getName());
            }
            String version = System.getenv(OCI_RESOURCE_PRINCIPAL_VERSION);
            if (version == null) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_VERSION +
                                " environment variable missing");
            }

            switch (version) {
                case RP_VERSION_1_1:
                    final String resourcePrincipalRptEndpoint =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT);
                    final String resourcePrincipalRpstEndpoint =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT);
                    return build_1_1(
                            resourcePrincipalRptEndpoint, resourcePrincipalRpstEndpoint);
                case RP_VERSION_2_1:
                case RP_VERSION_2_1_1:
                    final String resourcePrincipalRptEndpointFor2_1_or_2_1_1 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT);
                    final String resourcePrincipalRpstEndpointForLeafResourceFor2_1_or_2_1_1 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT);
                    final String resourcePrincipalResourceIdForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RESOURCE_ID);
                    final String resourcePrincipalTenancyIdForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_TENANCY_ID);
                    final String resourcePrincipalPrivateKeyForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM);
                    final String resourcePrincipalPassphraseForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
                    return build_2_1_or_2_1_1(
                            resourcePrincipalRptEndpointFor2_1_or_2_1_1,
                            resourcePrincipalRpstEndpointForLeafResourceFor2_1_or_2_1_1,
                            resourcePrincipalResourceIdForLeafResource,
                            resourcePrincipalTenancyIdForLeafResource,
                            resourcePrincipalPrivateKeyForLeafResource,
                            resourcePrincipalPassphraseForLeafResource,
                            version);
                case RP_VERSION_2_1_2:
                    final String resourcePrincipalRptEndpointFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT);
                    final String resourcePrincipalRpstEndpointForLeafResourceFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT);
                    final String resourcePrincipalResourceIdForLeafResourceFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RESOURCE_ID);
                    final String resourcePrincipalTenancyIdForLeafResourceFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_TENANCY_ID);
                    final String resourcePrincipalSecurityContext =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_SECURITY_CONTEXT);
                    final String resourcePrincipalPrivateKeyForLeafResourceFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM);
                    final String resourcePrincipalPassphraseForLeafResourceFor2_1_2 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
                    final String resourcePrincipalTokenPath =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_PATH);
                    return build_2_1_2(
                            resourcePrincipalRptEndpointFor2_1_2,
                            resourcePrincipalRpstEndpointForLeafResourceFor2_1_2,
                            resourcePrincipalTokenPath,
                            resourcePrincipalSecurityContext,
                            resourcePrincipalResourceIdForLeafResourceFor2_1_2,
                            resourcePrincipalTenancyIdForLeafResourceFor2_1_2,
                            resourcePrincipalPrivateKeyForLeafResourceFor2_1_2,
                            resourcePrincipalPassphraseForLeafResourceFor2_1_2,
                            version);
                case RP_VERSION_2_2:
                    final String resourcePrincipalPrivateKey =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM);
                    final String resourcePrincipalPassphrase =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
                    final String resourcePrincipalRegion =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_REGION);
                    final String resourcePrincipalSessionToken =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST);
                    return build_2_2(
                            resourcePrincipalPrivateKey,
                            resourcePrincipalPassphrase,
                            resourcePrincipalRegion,
                            resourcePrincipalSessionToken);
                case RP_VERSION_3_0:
                    return build_3_0();
                default:
                    throw new IllegalArgumentException(
                            OCI_RESOURCE_PRINCIPAL_VERSION +
                                    " has unknown value " + version);
            }
        }

        /**
         * Helper method that interprets the runtime environment to build a v1.1-configured client
         *
         * @return ResourcePrincipalProvider
         */
        public ResourcePrincipalProvider build_1_1(
                 String ociResourcePrincipalRptEndpoint, String ociResourcePrincipalRpstEndpoint
        ) {
            resourcePrincipalTokenEndpoint = ociResourcePrincipalRptEndpoint;
            if (ociResourcePrincipalRpstEndpoint != null) {
                federationEndpoint = ociResourcePrincipalRpstEndpoint;
            }

            // auto detect endpoint and region
            autoDetectEndpointUsingMetadataUrl();

            tokenSupplier = createTokenSupplier(sessionKeySupplier);
            return new ResourcePrincipalProvider(
                    tokenSupplier, sessionKeySupplier, region);
        }

        /**
         * Helper method that interprets the runtime environment to build a v2.1 or 2.1.1-configured
         * client
         *
         * @return ResourcePrincipalProvider
         */
        public ResourcePrincipalProvider build_2_1_or_2_1_1(
                String resourcePrincipalRptEndpoint,
                String resourcePrincipalRpstEndpoint,
                String resourcePrincipalResourceId,
                String resourcePrincipalTenancyId,
                String resourcePrincipalPrivateKey,
                String resourcePrincipalPassphrase,
                String resourcePrincipalVersion) {

            if(isBlank(resourcePrincipalRptEndpoint)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalTokenEndpoint cannot be blank");
            }
            if(isBlank(resourcePrincipalRpstEndpoint)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalTokenRpstEndpoint cannot be blank");
            }
            if(isBlank(resourcePrincipalResourceId)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalResourceId cannot be blank");
            }
            if(isBlank(resourcePrincipalPrivateKey)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalPrivateKey cannot be blank");
            }
            if (resourcePrincipalVersion.equals("2.1.1")) {
                if(isBlank(resourcePrincipalTenancyId)) {
                    throw new IllegalArgumentException("required: " +
                            "ResourcePrincipalTenancyId cannot be blank");
                }
            }

            sessionKeySupplier =
                    getSessionKeySupplierFromPemAndPassphrase(
                            resourcePrincipalPrivateKey,
                            resourcePrincipalPassphrase
                    );

            KeyPairProvider provider =
                    getKeyPairProvider(
                            resourcePrincipalResourceId,
                            resourcePrincipalPrivateKey,
                            resourcePrincipalPassphrase,
                            resourcePrincipalTenancyId,
                            resourcePrincipalVersion);

            String resourcePrincipalTokenPath = createTokenPath(
                    DEFAULT_OCI_RESOURCE_PRINCIPAL_RPT_PATH_FORV2_1_OR_2_1_1,
                    resourcePrincipalResourceId
            );

            tokenSupplier =
                    new ResourcePrincipalV2TokenSupplier(
                            resourcePrincipalRptEndpoint,
                            resourcePrincipalRpstEndpoint,
                            resourcePrincipalTokenPath,
                            sessionKeySupplier,
                            provider,
                            null);

            // auto detect region
            autoDetectEndpointUsingMetadataUrl();

            return new ResourcePrincipalProvider(
                    tokenSupplier, sessionKeySupplier, region);
        }

        /**
         * Helper method that interprets the runtime environment to build a v2.1.2-configured client
         *
         * @return ResourcePrincipalProvider
         */
        public ResourcePrincipalProvider build_2_1_2(
                String resourcePrincipalRptEndpoint,
                String resourcePrincipalRpstEndpoint,
                String resourcePrincipalTokenPath,
                String securityContext,
                String resourcePrincipalResourceId,
                String resourcePrincipalTenancyId,
                String resourcePrincipalPrivateKey,
                String resourcePrincipalPassphrase,
                String resourcePrincipalVersion) {

            if(isNotBlank(this.securityContext)) {
                securityContext = this.securityContext;
                logTrace(logger, "Security context provided via the builder overrides" +
                        " the value provided via environment variable");
            }
            if(isBlank(resourcePrincipalTokenPath)) {
                resourcePrincipalTokenPath = DEFAULT_OCI_RESOURCE_PRINCIPAL_RPT_PATH_FORV212;
            }

            if(isBlank(resourcePrincipalRptEndpoint)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalTokenEndpoint cannot be blank");
            }
            if(isBlank(resourcePrincipalRpstEndpoint)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalTokenRpstEndpoint cannot be blank");
            }
            if(isBlank(securityContext)) {
                throw new IllegalArgumentException("required: " +
                        "securityContext cannot be blank");
            }
            if(isBlank(resourcePrincipalResourceId)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalResourceId cannot be blank");
            }
            if(isBlank(resourcePrincipalPrivateKey)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalPrivateKey cannot be blank");
            }
            if(isBlank(resourcePrincipalTenancyId)) {
                throw new IllegalArgumentException("required: " +
                        "ResourcePrincipalTenancyId cannot be blank");
            }


            sessionKeySupplier =
                    getSessionKeySupplierFromPemAndPassphrase(
                            resourcePrincipalPrivateKey,
                            resourcePrincipalPassphrase
                    );

            KeyPairProvider provider =
                    getKeyPairProvider(
                            resourcePrincipalResourceId,
                            resourcePrincipalPrivateKey,
                            resourcePrincipalPassphrase,
                            resourcePrincipalTenancyId,
                            resourcePrincipalVersion);


            resourcePrincipalTokenPath =
                    createTokenPath(resourcePrincipalTokenPath, resourcePrincipalResourceId);

            tokenSupplier =
                    new ResourcePrincipalV2TokenSupplier(
                            resourcePrincipalRptEndpoint,
                            resourcePrincipalRpstEndpoint,
                            resourcePrincipalTokenPath,
                            sessionKeySupplier,
                            provider,
                            securityContext);

            // auto detect region
            autoDetectEndpointUsingMetadataUrl();

            return new ResourcePrincipalProvider(
                    tokenSupplier, sessionKeySupplier, region);
        }

        public ResourcePrincipalProvider build_2_2(
                String rpPrivateKey,
                String kp,
                String rpRegion,
                String rpst
        ) {
            final SessionKeyPairSupplier sessKeySupplier =
                    getSessionKeySupplierFromPemAndPassphrase(rpPrivateKey, kp);

            if (rpst == null) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_RPST + " environment variable missing");
            }
            final TokenSupplier tokenSupplier;
            if (new File(rpst).isAbsolute()) {
                logTrace(logger, "Valid file for RPST." +
                        " Creating instance of FileSecurityTokenSupplier");
                tokenSupplier = new FileSecurityTokenSupplier(
                        sessKeySupplier, rpst);
            } else {
                logTrace(logger, "Loading RPST from content provided. Creating instance of" +
                        " FixedSecurityTokenSupplier");
                tokenSupplier = new FixedSecurityTokenSupplier(
                        sessKeySupplier, rpst);
            }

            if (rpRegion == null) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_REGION + " environment variable missing");
            }
            final Region region = Region.fromRegionId(rpRegion);
            return new ResourcePrincipalProvider(tokenSupplier, sessKeySupplier, region);
        }

        /**
         * Helper method that interprets the runtime environment to build a v3.0-configured client
         *
         * @return ResourcePrincipalAuthenticationDetailsProvider
         */
        public ResourcePrincipalProvider build_3_0() {
            return ResourcePrincipalV3Provider.builder().build();
        }

        private KeyPairProvider getKeyPairProvider(
                String resourcePrincipalResourceId,
                String resourcePrincipalPrivateKey,
                String resourcePrincipalPassphrase,
                String tenancyId,
                String resourcePrincipalVersion) {
            final InputStream privateKeyStream;
            final String passphrase;
            if (new File(resourcePrincipalPrivateKey).isAbsolute()) {
                if (resourcePrincipalPassphrase != null
                        && !new File(resourcePrincipalPassphrase).isAbsolute()) {
                    throw new IllegalArgumentException(
                            "cannot mix path and constant settings for " +
                                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM + " and " +
                                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
                }
                try {
                    privateKeyStream = new FileInputStream(resourcePrincipalPrivateKey);
                    Path passphrasePath =
                            (resourcePrincipalPassphrase != null)
                                    ? new File(resourcePrincipalPassphrase).toPath()
                                    : null;
                    if (passphrasePath != null) {
                        passphrase = new String(Files.readAllBytes(passphrasePath));
                    } else passphrase = null;
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException("Can't find file for private key", e);
                } catch (IOException e) {
                    throw new RuntimeException("cannot read the passphrase", e);
                }

            } else {
                passphrase = resourcePrincipalPassphrase;
                privateKeyStream =
                        new ByteArrayInputStream(resourcePrincipalPrivateKey.getBytes());
            }

            return new KeyPairProvider(
                    resourcePrincipalResourceId,
                    privateKeyStream,
                    (passphrase != null) ? passphrase.toCharArray() : null,
                    tenancyId,
                    resourcePrincipalVersion);
        }

        private ResourcePrincipalV1TokenSupplier createTokenSupplier(
                SessionKeyPairSupplier sessionKeyPairSupplier) {
            createRptPathProvider();

            InstancePrincipalsProvider provider =
                    InstancePrincipalsProvider.builder()
                            .setFederationEndpoint(federationEndpoint)
                            .setLeafCertificateSupplier(leafCertificateSupplier)
                            .setIntermediateCertificateSuppliers(intermediateCertificateSuppliers)
                            // InstancePrincipalsProvider and
                            // ResourcePrincipalV1TokenSupplier's
                            // sessionKeysSupplier must be different. BTW
                            // FederationSecurityTokenSupplier and
                            // ResourcePrincipalProvider's sessionKeysSupplier
                            // must be same.
                            .build();

            // Prepare the provider to build the underlying HTTP client.
            provider.prepare(new NoSQLHandleConfig(federationEndpoint));

            String resourcePrincipalTokenPath = resourcePrincipalTokenPathProvider.getPath();

            return new ResourcePrincipalV1TokenSupplier(
                    resourcePrincipalTokenEndpoint,
                    federationEndpoint,
                    resourcePrincipalTokenPath,
                    sessionKeyPairSupplier,
                    provider);
        }

        private String createTokenPath(String resourcePrincipalTokenPath, String resourceId) {
            return "/" + resourcePrincipalTokenPath +
                    "/" + resourceId;
        }

        protected void createRptPathProvider() {
            if (resourcePrincipalTokenPathProvider == null) {
                resourcePrincipalTokenPathProvider = new RptPathProvider.DefaultRptPathProvider();
            }
        }
    }

    protected static SessionKeyPairSupplier getSessionKeySupplierFromPemAndPassphrase(
            String resourcePrincipalPrivateKey,
            String resourcePrincipalPassphrase) {
        SessionKeyPairSupplier sessKeySupplier;
        if (resourcePrincipalPrivateKey == null) {
            throw new IllegalArgumentException(
                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM +
                            " environment variable missing");
        }

        if (new File(resourcePrincipalPrivateKey).isAbsolute()) {
            if (resourcePrincipalPassphrase != null
                    && !new File(resourcePrincipalPassphrase).isAbsolute()) {
                throw new IllegalArgumentException(
                        "cannot mix path and constant settings for " +
                                OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM + " and " +
                                OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
            }
            logTrace(logger,"Valid file for private key." +
                    " Creating instance of FileKeyPairSupplier");
            sessKeySupplier = new FileKeyPairSupplier(
                    resourcePrincipalPrivateKey, resourcePrincipalPassphrase);
        } else {
            char[] passPhraseChars = null;
            if (resourcePrincipalPassphrase != null) {
                passPhraseChars = resourcePrincipalPassphrase.toCharArray();
            }
            logTrace(logger, "Invalid file for private key, using the" +
                    " content provided. Creating instance of FixedKeyPairSupplier");
            sessKeySupplier = new FixedKeyPairSupplier(resourcePrincipalPrivateKey,
                    passPhraseChars);
        }
        return sessKeySupplier;
    }
}
