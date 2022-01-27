/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.kv;

import oracle.nosql.driver.NoSQLException;

/**
 * On-premise only.
 * <p>
 * This exception is thrown when use StoreAccessTokenProvider in following
 * cases:
 * <ul>
 * <li>Authentication information was not provided in the request header
 * </li>
 * <li>The authentication session has expired. By default
 *    {@link StoreAccessTokenProvider} will automatically retry
 * authentication operation based on its authentication information</li>
 * </ul>
 */
public class AuthenticationException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public AuthenticationException(String msg) {
        super(msg);
    }

    /**
     * @hidden
     * @param msg the exception message
     * @param cause the cause
     */
    public AuthenticationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
