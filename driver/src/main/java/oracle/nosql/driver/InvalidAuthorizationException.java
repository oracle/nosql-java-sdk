/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The exception is thrown if the application presents an invalid authorization
 * string in a request.
 */
public class InvalidAuthorizationException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public InvalidAuthorizationException(String msg) {
        super(msg);
    }
}
