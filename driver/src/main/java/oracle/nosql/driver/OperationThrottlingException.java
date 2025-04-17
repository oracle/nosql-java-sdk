/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * An exception that is thrown when a non-data operation is throttled.
 * This can happen if an application attempts too many control operations
 * such as table creation, deletion, or similar methods. Such operations
 * do not use throughput or capacity provisioned for a given table but
 * they consume system resources and their use is limited.
 * <p>
 * Operations resulting in this exception can be retried but it is recommended
 * that callers use a relatively large delay before retrying in order to
 * minimize the chance that a retry will also be throttled.
 */
public class OperationThrottlingException extends ThrottlingException {

    private static final long serialVersionUID = 1L;

    /**
     * Internal use only
     * @param msg the exception message
     * @hidden
     */
    public OperationThrottlingException(String msg) {
        super(msg);
    }
}
