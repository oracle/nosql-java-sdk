/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam.pki;

public class PemEncryptedKeyException extends PemEncryptionException {
    /**
     * @hidden
     */
    PemEncryptedKeyException() {
        super("Private Key is encrypted, but no passphrase configured");
    }
}
