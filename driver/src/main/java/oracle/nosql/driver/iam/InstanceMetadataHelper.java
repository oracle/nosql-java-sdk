/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.*;
import static oracle.nosql.driver.util.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;
import static oracle.nosql.driver.util.HttpConstants.CONTENT_TYPE;

import java.io.IOException;
import java.util.logging.Logger;

import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.util.HttpRequestUtil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * @hidden
 * Internal use only
 * <p>
 * Helper class to fetch instance metadata from metadata service URL.
 */
class InstanceMetadataHelper {
    private static final JsonFactory factory = new JsonFactory();

    /* Instance metadata service base URL */
    private static final String METADATA_SERVICE_BASE_URL =
        "http://169.254.169.254/opc/v2/";
    private static final String FALLBACK_METADATA_SERVICE_URL =
        "http://169.254.169.254/opc/v1/";

    /* The authorization header need to send to metadata service since V2 */
    static final String AUTHORIZATION_HEADER_VALUE = "Bearer Oracle";
    private static final String METADATA_SERVICE_HOST =
        "169.254.169.254";

    static String getInstanceMetadaURL(String baseMetadataURL) {
        return baseMetadataURL + "instance/";
    }

    /**
     * Fetch the instance metadata.
     *
     * @param timeout request timeout
     * @param logger logger
     */
    static InstanceMetadata fetchMetadata(int timeout, Logger logger) {
        String baseMetadataURL = METADATA_SERVICE_BASE_URL;
        String instanceMDURL = getInstanceMetadaURL(baseMetadataURL);
        logTrace(logger, "Fetch instance metadata using " + instanceMDURL);
        HttpClient client = null;
        try {
            client = HttpClient.createMinimalClient(METADATA_SERVICE_HOST,
                80,
                null,
                0,
                "InstanceMDClient",
                logger);

            HttpRequestUtil.HttpResponse response = HttpRequestUtil.doGetRequest
                (client, instanceMDURL, headers(), timeout, logger);

            int status = response.getStatusCode();
            if (status == 404) {
                logTrace(logger, "Falling back to v1 metadata URL, " +
                    "resource not found from v2");
                baseMetadataURL = FALLBACK_METADATA_SERVICE_URL;
                instanceMDURL = getInstanceMetadaURL(baseMetadataURL);
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
            String insRegion = findKeyValue("canonicalRegionName", response.getOutput());
            String instanceId = findKeyValue("id", response.getOutput());
            logTrace(logger, "Instance region: " + insRegion +
                    ", Instance id: " + instanceId);
            return new InstanceMetadata(insRegion, instanceId, baseMetadataURL);
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

    private static String findKeyValue(String key, String response) {
        try {
            JsonParser parser = factory.createParser(response);
            if (parser.getCurrentToken() == null) {
                parser.nextToken();
            }
            while (parser.getCurrentToken() != null) {
                String field = findField(
                    response, parser, key);
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

    static class InstanceMetadata {
        private final String region;
        private final String id;
        private final String baseMetadataURL;

        InstanceMetadata(String region, String id, String baseMetadataURL) {
            this.region = region;
            this.id = id;
            this.baseMetadataURL = baseMetadataURL;
        }

        String getRegion() {
            return region;
        }

        String getId() {
            return id;
        }

        String getBaseURL() {
            return baseMetadataURL;
        }
    }
}
