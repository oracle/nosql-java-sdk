/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted is not supported. This may be related to
 * on-premises vs cloud service configurations.
 */
public class OperationNotSupportedException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * Internal use only
     * @param msg the exception message
     * @hidden
     */
    public OperationNotSupportedException(String msg) {
        super(msg);
    }
}
