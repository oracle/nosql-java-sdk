/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TokenRedactionTest {

    @Test
    public void testTokenResponseParseFailuresDoNotExposeResponse() {
        final String response = "{\"unexpected\":\"secret-token\"}";

        assertRedacted(() -> Utils.parseTokenResponse(response), response);
        assertRedacted(() -> Utils.parseResourcePrincipalTokenResponse(response),
                       response);
    }

    @Test
    public void testMalformedTokenResponseDoesNotExposeResponseInCause() {
        final String response = "{secret-token";

        assertRedacted(() -> Utils.parseTokenResponse(response), response);
        assertRedacted(() -> Utils.parseResourcePrincipalTokenResponse(response),
                       response);
    }

    private void assertRedacted(ThrowingRunnable runnable, String secret) {
        try {
            runnable.run();
            fail("Expected token parsing failure");
        } catch (IllegalStateException ise) {
            assertFalse(ise.getMessage().contains(secret));
            assertFalse(ise.getMessage().contains("secret-token"));
            assertFalse(ise.getCause() != null);
        }
    }

    private interface ThrowingRunnable {
        void run();
    }
}
