/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

import io.netty.handler.codec.http.HttpHeaders;
import oracle.nosql.driver.ops.Request;

/**
 * A callback interface used by the driver to obtain an authorization string
 * for a request. {@link NoSQLHandle} calls this interface when and
 * authorization string is required. In general applications need not implement
 * this interface, instead using the default mechanisms.
 * <p>
 * Instances of this interface must be reentrant and thread-safe.
 */
public interface AuthorizationProvider {

    /**
     * Returns an authorization string for specified request. This is sent to
     * the server in the request for authorization. Authorization information
     * can be request-dependent.
     *
     * @param request the request being processed
     *
     * @return a string indicating that the application is authorized to perform
     * the request
     */
    public String getAuthorizationString(Request request);

    /**
     * Release resources provider is using.
     */
    public void close();

    /**
     * Validates the authentication string. This method is optional and
     * by default it will not allow a null string.
     *
     * @param input the string to authorize
     *
     * @throws IllegalArgumentException if the authentication string is null
     */
    public default void validateAuthString(String input) {
        if (input == null) {
            throw new IllegalArgumentException(
                "Configured AuthorizationProvider acquired an " +
                "unexpected null authorization string");
        }
    }

    /**
     * Set HTTP headers required by the provider.
     *
     * @param authString the authorization string for the request
     *
     * @param request the request being processed
     *
     * @param headers the HTTP headers
     *
     * @param content the request content bytes
     */
    public default void setRequiredHeaders(String authString,
                                           Request request,
                                           HttpHeaders headers,
                                           byte[] content) {
        if (authString != null) {
            headers.set(AUTHORIZATION, authString);
        }
    }

    /**
     * Invalidate any cached authorization strings.
     */
    public default void flushCache() {
    }

    /**
     * Indicates whether or not the instance is used for the cloud
     * service
     *
     * @return false by default
     */
    public default boolean forCloud() {
        return false;
    }
}
