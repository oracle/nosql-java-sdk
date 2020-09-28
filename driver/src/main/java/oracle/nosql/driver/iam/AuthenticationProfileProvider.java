/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.InputStream;

/**
 * @hidden
 * Internal use only
 * <p>
 * The provider to supplies key id and private key that ared used to generate
 * request signature.
 */
interface AuthenticationProfileProvider {

    /**
     * Returns the keyId used to sign requests.
     * @return The keyId.
     */
    String getKeyId();


     /**
     * Returns a new InputStream of the private key. This stream should be
     * closed by the caller, implementations should return new streams each
     * time.
     *
     * @return A new InputStream.
     */
    InputStream getPrivateKey();


     /**
     * Returns the optional passphrase for the (encrypted) private key,
     * as a character array.
     *
     * @return The passphrase as character array, or null if not applicable
     */
    char[] getPassphraseCharacters();
}
