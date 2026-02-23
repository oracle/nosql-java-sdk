/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Thrown when the proxy reports a server-side FaultException that appears to
 * have originated from a remote networking/transport failure.
 * <p>
 * This exception is intentionally <em>not</em> retryable by the Java SDK's
 * internal retry handler; higher-level logic (tests, applications) can decide
 * whether and how to retry.
 */
public class ProxyRemoteException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    public ProxyRemoteException(String msg) {
        super(msg);
    }

    public ProxyRemoteException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
