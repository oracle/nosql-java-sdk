/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * Thrown to indicate that the size of a Request exceeds the system defined
 * limit.
 */
public class RequestSizeLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * @param msg the exception message
     */
    public RequestSizeLimitException(String msg) {
        super(msg);
    }
}
