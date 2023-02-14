/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The operation attempted to access a index that does not exist
 * or is not in a visible state.
 */
public class IndexNotFoundException extends ResourceNotFoundException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public IndexNotFoundException(String msg) {
        super(msg);
    }
}
