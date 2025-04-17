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
 * An exception that is thrown when security information is not ready in the
 * system. This exception will occur as the system acquires security information
 * and must be retried in order for authorization to work properly.
 */
public class SecurityInfoNotReadyException extends RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * Simple constructor
     * @param msg the message
     */
    public SecurityInfoNotReadyException(String msg) {
        super(msg);
    }

    /**
     * @hidden
     * Constructor with cause
     * @param msg the message
     * @param cause the cause
     */
    public SecurityInfoNotReadyException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
