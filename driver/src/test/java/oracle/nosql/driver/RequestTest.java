/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;

import oracle.nosql.driver.ops.QueryRequest;

import org.junit.Test;

public class RequestTest {

    @Test
    public void testQueryRequestCopyPreservesDurability() {
        QueryRequest request = new QueryRequest()
            .setDurability(Durability.COMMIT_SYNC);

        assertEquals(Durability.COMMIT_SYNC,
                     request.copyInternal().getDurability());
        assertEquals(Durability.COMMIT_SYNC,
                     request.copy().getDurability());
    }
}
