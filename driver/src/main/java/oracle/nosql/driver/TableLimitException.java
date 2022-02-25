/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * Thrown to indicate that an attempt has been made to create a number of tables
 * that exceeds the system defined limit.
 */
public class TableLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public TableLimitException(String msg) {
        super(msg);
    }
}
