/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam.pki;

/**
 * Denotes a type which holds sensitive data and must be erased once it has been used.
 *
 * <h3>Usage Model</h3>
 *
 * Use the try-with-resources idiom to assure the sensitive data is erased after use:
 *
 * <pre>
 *     try ( Sensitive sensitive = ... ) {
 *         // use the sensitive data
 *         ...
 *     }
 * </pre>
 */
interface Sensitive extends AutoCloseable {
    /** Must erase the contents of the passphrase */
    @Override
    void close();
}
