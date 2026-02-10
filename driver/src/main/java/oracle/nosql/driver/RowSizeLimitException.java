/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * Thrown to indicate that an attempt has been made to create a row with a
 * size that exceeds the system defined limit.
 */
public class RowSizeLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public RowSizeLimitException(String msg) {
        super(msg);
    }
}
