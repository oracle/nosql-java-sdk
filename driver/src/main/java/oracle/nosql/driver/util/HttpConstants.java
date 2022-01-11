/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * Constants used for HTTP headers and paths
 */
public class HttpConstants {

    /**
     * The http header that identifies the client scoped unique request id
     * associated with each request. The request header is returned by the
     * server, as part of the response and serves to associate the response
     * with the request.
     *
     * Note: We could use stream ids to associate a request and response.
     * However, the current handler pipeline on the client side operates at the
     * http level rather than the frame level, and consequently does not have
     * access to the stream id.
     */
    public static final String REQUEST_ID_HEADER = "x-nosql-request-id";

    /**
     * The version number associated with the serialization. The server will
     * use this version number when deserializing the http request and
     * serializing the http response.
     */
    public static final String REQUEST_SERDE_VERSION_HEADER =
        "x-nosql-serde-version";

    /**
     * A header for transferring a LogContext on an http request.
     */
    public static final String REQUEST_LOGCONTEXT_HEADER =
        "x-nosql-logcontext";

    /**
     * A header for transferring the compartment id on an http request.
     */
    public static final String REQUEST_COMPARTMENT_ID = "x-nosql-compartment-id";

    /**
     * Headers possibly set by the load balancer service to indicate original
     * IP address
     */
    public static final String X_REAL_IP_HEADER = "x-real-ip";
    public static final String X_FORWARDED_FOR_HEADER = "x-forwarded-for";

    /**
     * The name of the content type header
     */
    public static final String CONTENT_TYPE = "Content-Type";

    /**
     * The name of the content length header
     */
    public static final String CONTENT_LENGTH = "Content-Length";

    /**
     * The name of the date header
     */
    public static final String DATE = "date";

    /**
     * The name of the (request-target) header
     */
    public static final String REQUEST_TARGET = "(request-target)";

    /**
     * The name of the host header
     */
    public static final String HOST = "host";

    /**
     * the name of the content sha256 header
     */
    public static final String CONTENT_SHA = "x-content-sha256";

    /*
     * Keep alive header
     */
    public static final String KEEP_ALIVE = "keep-alive";

    public static final String CONNECTION = "Connection";

    public static final String ACCEPT = "Accept";

    public static final String USER_AGENT = "User-Agent";

    /*
     * NoSQL versions header
     */
    public static final String PROXY_VERSION_HEADER = "x-nosql-version";

    /*
     * Content type values
     */
    public static final String APPLICATION_JSON =
        "application/json; charset=UTF-8";
    /* should this be "binary" ? */
    public static final String OCTET_STREAM = "application/octet-stream";

    /*
     * Headers required for security. These need to be in each response
     */
    public static final String X_CONTENT_TYPE_OPTIONS = "X-content-type-options";
    public static final String X_CONTENT_TYPE_OPTIONS_VALUE = "nosniff";
    public static final String CONTENT_DISPOSITION = "content-disposition";
    public static final String CONTENT_DISPOSITION_VALUE =
        "attachment; filename=api.json";

    /*
     * The name of the Authorization header
     */
    public static final String AUTHORIZATION = "Authorization";

    /*
     * The Access Token prefix in authorization header
     */
    public static final String TOKEN_PREFIX = "Bearer ";

    public static final String userAgent = makeUserAgent();

    /*
     * Path Components
     */

    /**
     * The current version of the protocol
     */
    public static final String NOSQL_VERSION = "V2";

    /**
     * The path denoting a NoSQL request
     */
    public static final String NOSQL_DATA_PATH = makePath(NOSQL_VERSION,
                                                          "nosql/data");

    /**
     * The base path to the on-premise security services. All users need
     * a leading "/" so add it here.
     */
    public static final String KV_SECURITY_PATH = makePath("/" + NOSQL_VERSION,
                                                           "nosql/security");

    /**
     * Path component indicating table usage
     */
    public static final String TABLE_USAGE = "usage";

    /**
     * Path component indicating table history
     */
    public static final String TABLE_HISTORY = "history";

    /**
     * Path component indicating store info (internal use by tenant manager)
     */
    public static final String TABLE_STOREINFO = "storeinfo";

    /**
     * Path component indicating indexes operation
     */
    public static final String TABLE_INDEXES = "indexes";

    /*
     * Query Parameters used by GET operations
     */

    /**
     * Tenant id, required for all paths
     */
    public static final String TENANT_ID = "tenantid";

    /**
     * If exists, used for drop table, index
     */
    public static final String IF_EXISTS = "ifexists";

    /**
     * verb, used by retrieve request history
     */
    public static final String VERB = "verb";

    /**
     * Operation id, used optionally by GET table, calling the SC
     */
    public static final String OPERATION_ID = "operationid";

    /**
     * Used by list tables for paging (history, list)
     */
    public static final String START_INDEX = "start_index";

    /**
     * Used for numeric limits to return objects (usage, history, list)
     */
    public static final String LIMIT = "limit";

    /**
     * Used to specify a log level for LogControlService.
     */
    public static final String LOG_LEVEL = "level";

    /**
     * Used to specify an entrypoint for LogControlService.
     */
    public static final String ENTRYPOINT = "entrypoint";

    /**
     * Used by usage to return a range of records
     */
    public static final String START_TIMESTAMP = "start_timestamp";
    public static final String END_TIMESTAMP = "end_timestamp";

    /**
     * Used for paginating results that might be voluminous.
     */
    public static final String PAGE_SIZE = "pagesz";
    public static final String PAGE_NUMBER = "pageno";

    /**
     * Use this key to represent non-exist entry key for admin sub service
     * lookup.
     */
    public static final String NULL_KEY = "NULL";

    /**
     * Prefix of the basic authentication information.
     */
    public static final String BASIC_PREFIX = "Basic ";

    /**
     * Prefix of the authorization field for access token.
     */
    public static final String BEARER_PREFIX = "Bearer ";

    /**
     * Creates a URI path from the arguments
     */
    private static String makePath(String ... s) {
        StringBuilder sb = new StringBuilder();
        sb.append(s[0]);
        for (int i = 1; i < s.length; i++) {
            sb.append("/");
            sb.append(s[i]);
        }
        return sb.toString();
    }

    /**
     * Format: "NoSQL-JavaSDK/version (os info)"
     */
    public static String makeUserAgent() {
        String os = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        String javaVersion = System.getProperty("java.version");
        String javaVmName = System.getProperty("java.vm.name");
        StringBuilder sb = new StringBuilder();
        sb.append("NoSQL-JavaSDK/")
            .append(NoSQLHandleConfig.getLibraryVersion())
            .append(" (")
            .append(os).append("/").append(osVersion)
            .append("; ")
            .append(javaVersion).append("/").append(javaVmName)
            .append(")");
        return sb.toString();
    }
}
