/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted to create a resource but it already exists.
 */
public class ResourceExistsException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public ResourceExistsException(String msg) {
        super(msg);
    }
}
