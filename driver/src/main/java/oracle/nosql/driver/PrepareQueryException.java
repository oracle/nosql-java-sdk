/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * The query must be recompiled after DDL changes on the tables and/or
 * indexes that are accessed by the query.
 */
public class PrepareQueryException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public PrepareQueryException(String msg) {
        super(msg);
    }
}
