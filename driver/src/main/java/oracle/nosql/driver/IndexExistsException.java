/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted to create an index for a table but the named index
 * already exists.
 */
public class IndexExistsException extends ResourceExistsException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public IndexExistsException(String msg) {
        super(msg);
    }
}
