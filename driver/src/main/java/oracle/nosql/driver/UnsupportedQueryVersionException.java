/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * This exception is thrown if the server does not support the current
 * query protocol version.
 *
 * @since 5.4.14
 */
public class UnsupportedQueryVersionException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public UnsupportedQueryVersionException(String msg) {
        super(msg);
    }
}
