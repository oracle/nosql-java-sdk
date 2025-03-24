/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam.pki;

/**
 * Internal use
 * @hidden
 */
public class PemException extends IllegalStateException {

    PemException(final String message) {
        this(message, null);
    }

    PemException(final Throwable cause) {
        this(null, cause);
    }

    PemException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
