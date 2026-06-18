/*
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * An exception thrown when an operation is attempted on a replicated table
 * that is not yet fully initialized.
 *
 * @since 5.4.13
 */
public class TableNotReadyException extends RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public TableNotReadyException(String msg) {
        super(msg);
    }
}
