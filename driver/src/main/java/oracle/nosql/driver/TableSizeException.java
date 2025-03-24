/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * An exception indicating a table size limit has been exceeded by writing more
 * data than the table can support. This exception is not retryable because the
 * conditions that lead to it being thrown, while potentially transient,
 * typically require user intervention.
 */
public class TableSizeException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     *
     * Constructs an instance of <code>TableSizeException</code>.
     * @param msg the detail message
     */
    public TableSizeException(String msg) {
        super(msg);
    }
}
