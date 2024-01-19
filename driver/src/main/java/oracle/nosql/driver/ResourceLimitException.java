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
 * This is a base class for exceptions that result from reaching a limit for
 * a particular resource, such as number of tables, indexes, or a size limit
 * on data. It is never thrown directly.
 */
public class ResourceLimitException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     */
    protected ResourceLimitException(String msg) {
        super(msg);
    }
}
