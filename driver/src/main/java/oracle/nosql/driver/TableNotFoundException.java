/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted to access a table that does not exist
 * or is not in a visible state.
 */
public class TableNotFoundException extends ResourceNotFoundException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public TableNotFoundException(String msg) {
        super(msg);
    }
}
