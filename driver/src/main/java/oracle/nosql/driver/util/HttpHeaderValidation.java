/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

/**
 * @hidden
 */
public final class HttpHeaderValidation {

    private HttpHeaderValidation() {}

    public static void validateHttpHeaderValue(String fieldName,
                                               String value) {
        if (value == null) {
            return;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch <= 0x1f || ch == 0x7f) {
                throw new IllegalArgumentException(
                    fieldName +
                    " contains an invalid HTTP header value character " +
                    "0x" + toHex(ch) + " at index " + i);
            }
        }
    }

    private static String toHex(char ch) {
        String hex = Integer.toHexString(ch);
        return (hex.length() == 1) ? "0" + hex : hex;
    }
}
