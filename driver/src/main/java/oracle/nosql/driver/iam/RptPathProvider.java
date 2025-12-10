/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This path provider makes sure the behavior happens with the correct fallback.
 *
 * <p>For the path, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_PATH environment variable, if
 * set. Otherwise, use the current path: "/20180711/resourcePrincipalToken/{id}"
 *
 * <p>For the resource id, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_ID environment
 * variable, if set. Otherwise, use IMDS to get the instance id
 */
public abstract class RptPathProvider {
    private static final Logger logger = Logger.getLogger(RptPathProvider.class.getName());

    private static final String IMDS_PATH_TEMPLATE
            = "/20180711/resourcePrincipalToken/{id}";
    private static final int timeoutMs = 5_000;
    private final String pathTemplate;

    public RptPathProvider(String pathTemplate) {
        this.pathTemplate = pathTemplate;
    }

    public String getPath() {
        Map<String, String> replacements = getReplacements();
        String path = Utils.replace(pathTemplate, replacements, "{", "}");
        logTrace(logger,"Using path " + path);
        return path;
    }

    protected abstract Map<String, String> getReplacements();

    public static Map<String, String> buildEnvironmentRptPathProviderReplacements(
            String rptId) {
        if (rptId != null) {
            Map<String, String> replacements = new HashMap<>();
            replacements.put("id", rptId);
            return Collections.unmodifiableMap(replacements);
        } else {
            return null;
        }
    }

    public static Map<String, String> buildImdsRptPathProviderReplacements() {
        // Get instance Id from metadata service
        Map<String, String> replacements = new HashMap<>();
        replacements.put("id", InstanceMetadataHelper.
                fetchMetadata(timeoutMs, logger).getId());
        return Collections.unmodifiableMap(replacements);
    }

    /**
     * This path provider makes sure the behavior happens with the correct fallback.
     *
     * <p>For the path, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_PATH environment variable, if
     * set. Otherwise, use the current path: "/20180711/resourcePrincipalToken/{id}"
     *
     * <p>For the resource id, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_ID environment
     * variable, if set. Otherwise, use IMDS to get the instance id
     *
     * <p>This path provider is used when the caller doesn't provide a specific path provider to the
     * resource principals signer
     */
    public static class DefaultRptPathProvider extends RptPathProvider {
        static final String OCI_RESOURCE_PRINCIPAL_RPT_PATH
                = "OCI_RESOURCE_PRINCIPAL_RPT_PATH";
        static final String OCI_RESOURCE_PRINCIPAL_RPT_ID
                = "OCI_RESOURCE_PRINCIPAL_RPT_ID";
        private final Map<String, String> replacements;

        public DefaultRptPathProvider() {
            super(getPathTemplate());
            replacements = buildReplacements();
        }

        @Override
        protected Map<String, String> getReplacements() {
            return replacements;
        }

        private static String getPathTemplate() {
            String pathTemplate = System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_PATH);
            if (pathTemplate == null) {
                logTrace(logger,
                        "Unable to get path template from " +
                                OCI_RESOURCE_PRINCIPAL_RPT_PATH +
                                " env variable, using IMDS template"
                );
                pathTemplate = IMDS_PATH_TEMPLATE;
            }
            logTrace(logger, "The path template is " + pathTemplate);
            return pathTemplate;
        }

        private Map<String, String> buildReplacements() {
            String rptId = System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ID);
            Map<String, String> replacementMap = buildEnvironmentRptPathProviderReplacements(rptId);
            if (replacementMap == null) {
                logTrace(logger,
                        "Unable to get replacements from " +
                                OCI_RESOURCE_PRINCIPAL_RPT_ID +
                                "env variable, getting replacements from IMDS"
                );
                replacementMap = buildImdsRptPathProviderReplacements();
            }
            logTrace(logger, "The replacement map is " + replacementMap);
            return replacementMap;
        }
    }

    /**
     * This path provider makes sure the behavior happens with the correct fallback.
     *
     * <p>For the path, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_PATH_FOR_LEAF_RESOURCE
     * environment variable, if set. Otherwise, use the current path:
     * "/20180711/resourcePrincipalToken/{id}"
     *
     * <p>For the resource id, Use the contents of the OCI_RESOURCE_PRINCIPAL_RPT_ID_FOR_LEAF_RESOURCE
     * environment variable, if set. Otherwise, use IMDS to get the instance id
     *
     * <p>This path provider is used when the caller doesn't provide a specific path provider to the
     * resource principals signer
     */
    public static class DefaultLeafRptPathProvider extends RptPathProvider {

        static final String OCI_RESOURCE_PRINCIPAL_RPT_PATH_FOR_LEAF_RESOURCE =
                "OCI_RESOURCE_PRINCIPAL_RPT_PATH_FOR_LEAF_RESOURCE";
        static final String OCI_RESOURCE_PRINCIPAL_RPT_ID_FOR_LEAF_RESOURCE =
                "OCI_RESOURCE_PRINCIPAL_RPT_ID_FOR_LEAF_RESOURCE";
        private final Map<String, String> replacements;

        public DefaultLeafRptPathProvider() {
            super(getPathTemplate());
            replacements = buildReplacements();
        }

        @Override
        protected Map<String, String> getReplacements() {
            return replacements;
        }

        private static String getPathTemplate() {
            String pathTemplate = System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_PATH_FOR_LEAF_RESOURCE);
            if (pathTemplate == null) {
                logTrace(logger,
                        "Unable to get path template from " +
                                OCI_RESOURCE_PRINCIPAL_RPT_PATH_FOR_LEAF_RESOURCE +
                                " env variable, using IMDS template"
                );
                pathTemplate = IMDS_PATH_TEMPLATE;
            }
            logTrace(logger, "The path template is " + pathTemplate);
            return pathTemplate;
        }

        private Map<String, String> buildReplacements() {
            String rptId = System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ID_FOR_LEAF_RESOURCE);
            Map<String, String> replacementMap = buildEnvironmentRptPathProviderReplacements(rptId);
            if (replacementMap == null) {
                logTrace(logger,
                        "Unable to get replacements from " +
                                OCI_RESOURCE_PRINCIPAL_RPT_ID_FOR_LEAF_RESOURCE +
                                "env variable, getting replacements from IMDS"
                );
                replacementMap = buildImdsRptPathProviderReplacements();
            }
            logTrace(logger, "The replacement map is " + replacementMap);
            return replacementMap;
        }
    }
}
