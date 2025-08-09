/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Internal use only
 * @hidden
 *
 * An exception thrown when the binding operation of the transaction is still
 * in progress and has not completed.
 */
public class TxnBindingOpInprogressException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public TxnBindingOpInprogressException(String msg) {
        super(msg);
    }
}