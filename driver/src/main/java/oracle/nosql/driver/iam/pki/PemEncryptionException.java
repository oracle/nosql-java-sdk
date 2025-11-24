/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam.pki;

public class PemEncryptionException extends PemException {

    PemEncryptionException(Throwable cause) {
        super(cause);
    }

    PemEncryptionException(final String message) {
        super(message);
    }
}
