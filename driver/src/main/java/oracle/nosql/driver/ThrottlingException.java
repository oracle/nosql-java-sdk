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
 * ThrottlingException is a base class for exceptions that indicate the
 * application has exceeded a provisioned or implicit limit in terms
 * of size of data accessed or frequency of operation.
 * <p>
 * Operations resulting in this exception can be retried but it is recommended
 * that callers use a delay before retrying in order to minimize the chance
 * that a retry will also be throttled.
 * <p>
 * It is recommended that applications use rate limiting to avoid these
 * exceptions.
 */
public class ThrottlingException extends RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     */
    protected ThrottlingException(String msg) {
        super(msg);
    }
}
