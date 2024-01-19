/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * This exception is thrown if the server does not support the current
 * driver protocol version.
 *
 * @since 5.3.0
 */
public class UnsupportedProtocolException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public UnsupportedProtocolException(String msg) {
        super(msg);
    }
}
