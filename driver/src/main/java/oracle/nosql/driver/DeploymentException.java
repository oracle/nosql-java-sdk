/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * Thrown to indicate that an attempt has been made to create or modify a table
 * using limits that exceed the maximum allowed for a single table or that
 * cause the tenant's aggregate resources to exceed the maximum allowed for a
 * tenant. These are system-defined limits.
 */
public class DeploymentException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public DeploymentException(String msg) {
        super(msg);
    }
}
