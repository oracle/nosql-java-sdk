/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * Thrown to indicate that the number of operations included in
 * {@link NoSQLHandle#writeMultiple} operation exceeds the
 * system defined limit.
 */
public class BatchOperationNumberLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the message
     */
    public BatchOperationNumberLimitException(String msg) {
        super(msg);
    }
}
