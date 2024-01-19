/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * @hidden
 * Internal use only
 * <p>
 * The class to supplies private key from file.
 */
class PrivateKeyFileSupplier implements Supplier<InputStream> {

    private final File pemFile;

    public PrivateKeyFileSupplier(File pemFile) {
        this.pemFile = pemFile;
    }

    @Override
    public InputStream get() {
        try {
            return new FileInputStream(pemFile);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(
                "Could not find private key: " + pemFile, e);
        }
    }
}
