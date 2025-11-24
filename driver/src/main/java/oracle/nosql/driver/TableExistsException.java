/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted to create a table but the named table already exists.
 */
public class TableExistsException extends ResourceExistsException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public TableExistsException(String msg) {
        super(msg);
    }
}
