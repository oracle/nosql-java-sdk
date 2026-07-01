/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

/**
 * @hidden
 */
final class SystemStatementRedactor {

    private static final String IDENTIFIED = "identified";
    private static final String BY = "by";
    private static final String REDACTED = "<redacted>";

    private SystemStatementRedactor() {}

    static String redact(String statement) {
        if (statement == null) {
            return null;
        }
        int redactStart = getSensitiveValueStart(statement);
        if (redactStart < 0) {
            return statement;
        }
        return statement.substring(0, redactStart) + REDACTED;
    }

    static String redact(char[] statement) {
        if (statement == null) {
            return null;
        }
        int redactStart = getSensitiveValueStart(statement);
        if (redactStart < 0) {
            return new String(statement);
        }
        return new String(statement, 0, redactStart) + REDACTED;
    }

    private static int getSensitiveValueStart(CharSequence statement) {
        for (int i = 0; i < statement.length(); i++) {
            int pos = matchWord(statement, i, IDENTIFIED);
            if (pos < 0) {
                continue;
            }
            pos = skipWhitespace(statement, pos);
            pos = matchWord(statement, pos, BY);
            if (pos < 0) {
                continue;
            }
            return skipWhitespace(statement, pos);
        }
        return -1;
    }

    private static int getSensitiveValueStart(char[] statement) {
        for (int i = 0; i < statement.length; i++) {
            int pos = matchWord(statement, i, IDENTIFIED);
            if (pos < 0) {
                continue;
            }
            pos = skipWhitespace(statement, pos);
            pos = matchWord(statement, pos, BY);
            if (pos < 0) {
                continue;
            }
            return skipWhitespace(statement, pos);
        }
        return -1;
    }

    private static int matchWord(CharSequence value, int offset, String word) {
        int end = offset + word.length();
        if (end > value.length()) {
            return -1;
        }
        if (offset > 0 && isWordChar(value.charAt(offset - 1))) {
            return -1;
        }
        for (int i = 0; i < word.length(); i++) {
            if (Character.toLowerCase(value.charAt(offset + i)) !=
                word.charAt(i)) {
                return -1;
            }
        }
        if (end < value.length() && isWordChar(value.charAt(end))) {
            return -1;
        }
        return end;
    }

    private static int matchWord(char[] value, int offset, String word) {
        int end = offset + word.length();
        if (end > value.length) {
            return -1;
        }
        if (offset > 0 && isWordChar(value[offset - 1])) {
            return -1;
        }
        for (int i = 0; i < word.length(); i++) {
            if (Character.toLowerCase(value[offset + i]) != word.charAt(i)) {
                return -1;
            }
        }
        if (end < value.length && isWordChar(value[end])) {
            return -1;
        }
        return end;
    }

    private static int skipWhitespace(CharSequence value, int offset) {
        while (offset < value.length() &&
               Character.isWhitespace(value.charAt(offset))) {
            offset++;
        }
        return offset;
    }

    private static int skipWhitespace(char[] value, int offset) {
        while (offset < value.length && Character.isWhitespace(value[offset])) {
            offset++;
        }
        return offset;
    }

    private static boolean isWordChar(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '_';
    }
}
