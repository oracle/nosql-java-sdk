/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

public class LogUtilTest {

    @Test
    public void testRedactHeaderValue() {
        assertEquals("Bearer <redacted>",
                     LogUtil.redactHeaderValue(HttpConstants.AUTHORIZATION,
                                               "Bearer secret-token"));
        assertEquals("Basic <redacted>",
                     LogUtil.redactHeaderValue("Proxy-Authorization",
                                               "Basic dXNlcjpwYXNz"));
        assertEquals("<redacted>",
                     LogUtil.redactHeaderValue("opc-obo-token",
                                               "delegation-token"));
        assertEquals("visible",
                     LogUtil.formatHeaderValueForLog("X-Custom", "visible"));
    }

    @Test
    public void testFormatHeadersForLogRedactsSensitiveHeaders() {
        final HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpConstants.AUTHORIZATION, "Bearer secret-token");
        headers.add(HttpConstants.COOKIE, "session=abc123");
        headers.add("opc-obo-token", "delegation-token");
        headers.add("security-context", "opaque-security-context");
        headers.add("X-Custom", "visible");

        final String formatted = LogUtil.formatHeadersForLog(headers);

        assertTrue(formatted.contains("Authorization: Bearer <redacted>"));
        assertTrue(formatted.contains("Cookie: <redacted>"));
        assertTrue(formatted.contains("opc-obo-token: <redacted>"));
        assertTrue(formatted.contains("security-context: <redacted>"));
        assertTrue(formatted.contains("X-Custom: visible"));
        assertFalse(formatted.contains("secret-token"));
        assertFalse(formatted.contains("abc123"));
        assertFalse(formatted.contains("delegation-token"));
        assertFalse(formatted.contains("opaque-security-context"));
    }

    @Test
    public void testFormatHeadersForLogCanKeepSensitiveHeaders() {
        final HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpConstants.AUTHORIZATION, "Bearer secret-token");
        headers.add(HttpConstants.COOKIE, "session=abc123");

        final String formatted = LogUtil.formatHeadersForLog(headers, false);

        assertTrue(formatted.contains("Authorization: Bearer secret-token"));
        assertTrue(formatted.contains("Cookie: session=abc123"));
    }

    @Test
    public void testFormatHeadersForLogPreservesNonValidatingBehavior() {
        final HttpHeaders headers = new DefaultHttpHeaders(false);
        headers.add("Bad Header", "visible");
        headers.add(HttpConstants.AUTHORIZATION, "Bearer secret-token");

        final String formatted = LogUtil.formatHeadersForLog(headers);

        assertTrue(formatted.contains("Bad Header: visible"));
        assertTrue(formatted.contains("Authorization: Bearer <redacted>"));
    }

    @Test
    public void testFormatHeaderValueForLogCanSkipRedaction() {
        assertEquals("<redacted>",
                     LogUtil.formatHeaderValueForLog(HttpConstants.COOKIE,
                                                     "session=abc123"));
        assertEquals("session=abc123",
                     LogUtil.formatHeaderValueForLog(HttpConstants.COOKIE,
                                                     "session=abc123",
                                                     false));
    }
}
