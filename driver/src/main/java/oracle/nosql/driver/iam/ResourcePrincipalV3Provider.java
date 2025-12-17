/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.isBlank;
import static oracle.nosql.driver.iam.Utils.logTrace;

import java.io.File;

import oracle.nosql.driver.Region;

public class ResourcePrincipalV3Provider
            extends ResourcePrincipalProvider {
    private static final String OCI_RESOURCE_PRINCIPAL_VERSION_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_VERSION_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_LEAF_RESOURCE";
    private static final String RP_VERSION_1_1 = "1.1";
    private static final String OCI_RESOURCE_PRINCIPAL_RESOURCE_ID_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RESOURCE_ID_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPST_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RPST_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_REGION_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_REGION_FOR_LEAF_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPT_URL_FOR_PARENT_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RPT_URL_FOR_PARENT_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_PARENT_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_PARENT_RESOURCE";
    private static final String OCI_RESOURCE_PRINCIPAL_TENANCY_ID_FOR_LEAF_RESOURCE =
            "OCI_RESOURCE_PRINCIPAL_TENANCY_ID_FOR_LEAF_RESOURCE";

    /**
     * Constructor of ResourcePrincipalAuthenticationDetailsProvider.
     *
     * @param tokenSupplier token supplier implementation.
     * @param sessionKeyPairSupplier session key supplier implementation.
     * @param region the region
     */
    ResourcePrincipalV3Provider(TokenSupplier tokenSupplier,
                              SessionKeyPairSupplier sessionKeyPairSupplier,
                              Region region) {
        super(tokenSupplier, sessionKeyPairSupplier, region);
    }

    /**
     * Creates a new ResourcePrincipalProvider.
     *
     * @return A new builder instance.
     */
    public static ResourcePrincipalV3ProviderBuilder builder() {
        return new ResourcePrincipalV3ProviderBuilder();
    }

    /** Builder for ResourcePrincipalProviderBuilder. */
    public static class ResourcePrincipalV3ProviderBuilder
        extends ResourcePrincipalProviderBuilder {

        /**
         * The endpoint that can provide the parent resource principal token.
         *
         * <p>Required.
         */
        private String resourcePrincipalTokenUrlForParentResource;

        /** The federation endpoint/RPST endpoint for parent resource */
        private String federationEndpointForParentResource;

        /** Configures the resourcePrincipalTokenUrlForParentResource to use. */
        public ResourcePrincipalV3ProviderBuilder
                setResourcePrincipalTokenUrlForParentResource(
                        String resourcePrincipalTokenUrlForParentResource) {
            this.resourcePrincipalTokenUrlForParentResource =
                    resourcePrincipalTokenUrlForParentResource;
            return this;
        }

        /** Configures the resourcePrincipalFederationUrlForParentResource to use. */
        public ResourcePrincipalV3ProviderBuilder
                setFederationEndpointForParentResource(String federationEndpointForParentResource) {
            this.federationEndpointForParentResource = federationEndpointForParentResource;
            return this;
        }

        /**
         * Build a new ResourcePrincipalsV3AuthenticationDetailsProvider.
         *
         * @return A new provider instance.
         */
        public ResourcePrincipalV3Provider build() {
            final String resourcePrincipalVersionForLeafResource =
                    System.getenv(OCI_RESOURCE_PRINCIPAL_VERSION_FOR_LEAF_RESOURCE);
            if (isBlank(resourcePrincipalVersionForLeafResource)) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_VERSION_FOR_LEAF_RESOURCE
                                + " environment variable missing");
            }
            ResourcePrincipalProvider leafResourceAuthProvider = null;
            switch (resourcePrincipalVersionForLeafResource) {
                case RP_VERSION_1_1:
                    final String resourcePrincipalRptEndpointForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalRpstEndpointForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_LEAF_RESOURCE);
                    leafResourceAuthProvider =
                            build_1_1(
                                    resourcePrincipalRptEndpointForLeafResource,
                                    resourcePrincipalRpstEndpointForLeafResource);
                    break;
                case RP_VERSION_2_1:
                case RP_VERSION_2_1_1:
                    final String resourcePrincipalRptEndpointForLeafResourceFor2_1_or_2_1_1 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ENDPOINT_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalRpstEndpointForLeafResourceFor2_1_or_2_1_1 =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalResourceIdForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RESOURCE_ID_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalTenancyIdForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_TENANCY_ID_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalPrivateKeyForLeafResource =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_FOR_LEAF_RESOURCE);
                    final String resourcePrincipalPassphraseForLeafResource =
                            System.getenv(
                                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE_FOR_LEAF_RESOURCE);
                    leafResourceAuthProvider =
                            build_2_1_or_2_1_1(
                                    resourcePrincipalRptEndpointForLeafResourceFor2_1_or_2_1_1,
                                    resourcePrincipalRpstEndpointForLeafResourceFor2_1_or_2_1_1,
                                    resourcePrincipalResourceIdForLeafResource,
                                    resourcePrincipalTenancyIdForLeafResource,
                                    resourcePrincipalPrivateKeyForLeafResource,
                                    resourcePrincipalPassphraseForLeafResource,
                                    resourcePrincipalVersionForLeafResource);
                    break;
                case RP_VERSION_2_2:
                    final String ociResourcePrincipalPrivateKey =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_FOR_LEAF_RESOURCE);
                    final String ociResourcePrincipalPassphrase =
                            System.getenv(
                                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE_FOR_LEAF_RESOURCE);
                    final String ociResourcePrincipalRpst =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_FOR_LEAF_RESOURCE);
                    final String ociResourcePrincipalRegion =
                            System.getenv(OCI_RESOURCE_PRINCIPAL_REGION_FOR_LEAF_RESOURCE);
                    leafResourceAuthProvider =
                            build_2_2_leaf(
                                    ociResourcePrincipalPrivateKey,
                                    ociResourcePrincipalPassphrase,
                                    ociResourcePrincipalRpst,
                                    ociResourcePrincipalRegion);
                    break;
                default:
                    throw new IllegalArgumentException(
                            OCI_RESOURCE_PRINCIPAL_VERSION_FOR_LEAF_RESOURCE
                                    + " has unknown value " + resourcePrincipalVersionForLeafResource);
            }
            return build(leafResourceAuthProvider);
        }

        /**
         * Helper method that interprets the runtime environment to build a v2.2-configured leaf
         * client
         *
         * @return ResourcePrincipalProvider
         */
        public ResourcePrincipalProvider build_2_2_leaf(
                String ResourcePrincipalPrivateKey,
                String ResourcePrincipalPassphrase,
                String ResourcePrincipalRpst,
                String ResourcePrincipalRegion
        ) {
            sessionKeySupplier =
                    getSessionKeySupplierFromPemAndPassphrase(
                            ResourcePrincipalPrivateKey, ResourcePrincipalPassphrase);

            if (ResourcePrincipalRpst == null) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_RPST + " environment variable missing");
            }
            if (new File(ResourcePrincipalRpst).isAbsolute()) {
                logTrace(logger, "Valid file for RPST." +
                        " Creating instance of FileSecurityTokenSupplier");
                tokenSupplier = new FileSecurityTokenSupplier(
                        sessionKeySupplier, ResourcePrincipalRpst);
            } else {
                logTrace(logger, "Loading RPST from content provided. Creating instance of" +
                        " FixedSecurityTokenSupplier");
                tokenSupplier = new FixedSecurityTokenSupplier(
                        sessionKeySupplier, ResourcePrincipalRpst);
            }

            if (ResourcePrincipalRegion == null) {
                throw new IllegalArgumentException(
                        OCI_RESOURCE_PRINCIPAL_REGION + " environment variable missing");
            }
            region = Region.fromRegionId(ResourcePrincipalRegion);
            return new ResourcePrincipalProvider(tokenSupplier, sessionKeySupplier, region);
        }

        /**
         * Builds a new instance of ResourcePrincipalsV3Provider
         *
         * @param leafResourceAuthProvider instance of
         *     ResourcePrincipalProvider for leaf resource
         */
        public ResourcePrincipalV3Provider build(
                ResourcePrincipalProvider leafResourceAuthProvider) {
            final String ociResourcePrincipalRptUrlForParentResource =
                    System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_URL_FOR_PARENT_RESOURCE);

            resourcePrincipalTokenUrlForParentResource =
                    resourcePrincipalTokenUrlForParentResource != null
                            ? resourcePrincipalTokenUrlForParentResource
                            : ociResourcePrincipalRptUrlForParentResource;

            if (resourcePrincipalTokenUrlForParentResource == null) {
                throw new NullPointerException(
                        "resourcePrincipalTokenUrlForParentResource must not be null");
            }

            final String ociResourcePrincipalRpstEndpointForParentResource =
                    System.getenv(OCI_RESOURCE_PRINCIPAL_RPST_ENDPOINT_FOR_PARENT_RESOURCE);

            federationEndpointForParentResource =
                    federationEndpointForParentResource != null
                            ? federationEndpointForParentResource
                            : ociResourcePrincipalRpstEndpointForParentResource;

            if (federationEndpointForParentResource == null) {
                federationEndpointForParentResource = autoDetectEndpointUsingMetadataUrl();
            }

            TokenSupplier federationClientForParentResource =
                    createFederationClientForParentResource(leafResourceAuthProvider);

            return new ResourcePrincipalV3Provider(
                    federationClientForParentResource, sessionKeySupplier, region);
        }

        private TokenSupplier createFederationClientForParentResource(
                ResourcePrincipalProvider leafResourceAuthProvider) {
            return new ResourcePrincipalV3TokenSupplier(
                    resourcePrincipalTokenUrlForParentResource,
                    federationEndpointForParentResource,
                    sessionKeySupplier,
                    leafResourceAuthProvider);
        }

        @Override
        protected void createRptPathProvider() {
            if(resourcePrincipalTokenEndpoint == null) {
                throw new NullPointerException(
                        "resourcePrincipalTokenEndpoint for leaf-resource must not be null");
            }
            if (resourcePrincipalTokenPathProvider == null) {
                resourcePrincipalTokenPathProvider = new RptPathProvider.DefaultLeafRptPathProvider();
            }
        }
    }


}
