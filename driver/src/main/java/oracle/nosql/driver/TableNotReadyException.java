/*
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * An exception that is thrown when the multi-region table is not ready for DML
 * operations before the initialization process complete.
 */
public class TableNotReadyException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public TableNotReadyException(String msg) {
        super(msg);
    }
}
