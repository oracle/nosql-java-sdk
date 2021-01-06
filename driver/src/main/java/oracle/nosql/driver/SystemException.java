/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * An exception that is thrown when there is an internal system problem.
 * Most system problems are temporary, so this is a retryable exception.
 */
public class SystemException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public SystemException(String msg) {
        super(msg);
    }
}
