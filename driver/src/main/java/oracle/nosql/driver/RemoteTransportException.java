/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Thrown when the service reports a server-side {@code FaultException} that
 * appears to have originated from a remote networking/transport failure
 * between service components.
 * <p>
 * This exception is intentionally <em>not</em> retried by the Java SDK's
 * internal retry handler. Application code may choose to retry, using the
 * following guidance:
 * <ul>
 *   <li><b>Idempotent operations</b>: it is generally safe to retry.</li>
 *   <li><b>Non-idempotent operations</b>: first perform a read to determine
 *       whether the operation may have already succeeded, and then retry
 *       only if necessary.</li>
 * </ul>
 */
public class RemoteTransportException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    public RemoteTransportException(String msg) {
        super(msg);
    }

    public RemoteTransportException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
