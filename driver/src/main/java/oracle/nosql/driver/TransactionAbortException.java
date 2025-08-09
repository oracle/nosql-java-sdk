/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * An exception thrown when a transaction cannot be committed for some reason
 * and was aborted, any changes made during the transaction were discarded.
 */
public class TransactionAbortException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    public TransactionAbortException(String msg) {
        super(msg);
    }
}