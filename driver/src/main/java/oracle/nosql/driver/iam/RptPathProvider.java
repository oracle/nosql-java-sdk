/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.util.HttpRequestUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.util.HttpConstants.*;


public class RptPathProvider {
    private static final Logger logger = Logger.getLogger(RptPathProvider.class.getName());

    static final String OCI_RESOURCE_PRINCIPAL_RPT_PATH
            = "OCI_RESOURCE_PRINCIPAL_RPT_PATH";
    static final String OCI_RESOURCE_PRINCIPAL_RPT_ID
            = "OCI_RESOURCE_PRINCIPAL_RPT_ID";
    static final String IMDS_PATH_TEMPLATE
            = "/20180711/resourcePrincipalToken/{id}";
    private static final String METADATA_SERVICE_BASE_URL
            = "http://169.254.169.254/opc/v2/";
    private static final String METADATA_SERVICE_HOST = "169.254.169.254";
    private static final String FALLBACK_METADATA_SERVICE_URL
            = "http://169.254.169.254/opc/v1/";
    static final String AUTHORIZATION_HEADER_VALUE = "Bearer Oracle";

    private final Map<String, String> replacements;
    private final String pathTemplate;

    public RptPathProvider() {
        this.pathTemplate = getPathTemplate();
        logTrace(logger, "A path provider was not specified, using DefaultRptPathProvider");
        replacements = buildReplacements();
    }

    public String getPath() {
        Map<String, String> replacements = getReplacements();
        String path = replace(pathTemplate, replacements, "{", "}");
        logTrace(logger,"Using path " + path);
        return path;
    }

    protected Map<String, String> getReplacements() {
        return replacements;
    }

    private String getPathTemplate() {
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
        Map<String, String> replacementMap = buildEnvironmentRptPathProviderReplacements();
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

    private Map<String, String> buildEnvironmentRptPathProviderReplacements() {
        String rptId = System.getenv(OCI_RESOURCE_PRINCIPAL_RPT_ID);
        if (rptId != null) {
            Map<String, String> replacements = new HashMap<>();
            replacements.put("id", rptId);
            return Collections.unmodifiableMap(replacements);
        } else {
            return null;
        }
    }

    private Map<String, String> buildImdsRptPathProviderReplacements() {
        // Get instance Id from metadata service
        Map<String, String> replacements = new HashMap<>();
        replacements.put("id", getInstanceIdFromIMDS());
        return Collections.unmodifiableMap(replacements);
    }

    private String getInstanceIdFromIMDS() {
        String baseMetadataURL = METADATA_SERVICE_BASE_URL;
        String instanceIdMDURL = baseMetadataURL + "instance/id";
        int timeout = 5_000;
        logTrace(logger, "Fetch instance metadata using " + instanceIdMDURL);
        HttpClient client = null;
        try {
            client = HttpClient.createMinimalClient(METADATA_SERVICE_HOST,
                    80,
                    null,
                    0,
                    "InstanceMDClient",
                    logger);

            HttpRequestUtil.HttpResponse response = HttpRequestUtil.doGetRequest
                    (client, instanceIdMDURL, headers(), timeout, logger);

            int status = response.getStatusCode();
            if (status == 404) {
                logTrace(logger, "Falling back to v1 metadata URL, " +
                        "resource not found from v2");
                baseMetadataURL = FALLBACK_METADATA_SERVICE_URL;
                instanceIdMDURL = baseMetadataURL + "instance/id";
                response = HttpRequestUtil.doGetRequest
                        (client, instanceIdMDURL, headers(), timeout, logger);
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
            return response.getOutput();
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private static HttpHeaders headers() {
        return new DefaultHttpHeaders()
                .set(CONTENT_TYPE, APPLICATION_JSON)
                .set(AUTHORIZATION, AUTHORIZATION_HEADER_VALUE);
    }

    /**
     * Replace the placeholders in the template with the values in the replacement mapping.
     *
     * @param template template string
     * @param replacements map from key to replacement value
     * @param prefix prefix of the placeholder
     * @param suffix suffix of the placeholder
     * @return replaced string
     */
    public static String replace(
            String template, Map<String, String> replacements, String prefix, String suffix) {
        String result = template;
        for (Map.Entry<String, String> e : replacements.entrySet()) {
            result =
                    result.replaceAll(
                            Pattern.quote(prefix)
                                    + Pattern.quote(e.getKey())
                                    + Pattern.quote(suffix),
                            e.getValue());
        }
        return result;
    }


}
