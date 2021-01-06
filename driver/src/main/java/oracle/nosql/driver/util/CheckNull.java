/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.util.Objects;

/**
 * Wrapper for calls to Objects.requireNonNull
 */
public class CheckNull {

    public static void requireNonNull(Object value, String message) {
        Objects.requireNonNull(value, message);
    }
}
