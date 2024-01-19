/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * @hidden
 * Internal use only
 * <p>
 * Supplier to provide private key in String format
 */
class PrivateKeyStringSupplier implements Supplier<InputStream>{
    private final String privateKey;

    public PrivateKeyStringSupplier(String privateKeyString) {
        this.privateKey = privateKeyString;
    }

    @Override
    public InputStream get() {
        return new ByteArrayInputStream(privateKey.getBytes());
    }
}
