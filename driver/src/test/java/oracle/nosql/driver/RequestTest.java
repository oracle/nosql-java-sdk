/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DropReplicaRequest;
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

    @Test
    public void testReplicaRequestsDoNotRetryWithoutIdempotencyToken() {
        RetryHandler handler = new DefaultRetryHandler(10, 0);
        RetryableException retryable = new SystemException("retryable");

        AddReplicaRequest addReplica = new AddReplicaRequest();
        assertFalse(addReplica.shouldRetry());
        assertFalse(handler.doRetry(addReplica, 0, retryable));

        DropReplicaRequest dropReplica = new DropReplicaRequest();
        assertFalse(dropReplica.shouldRetry());
        assertFalse(handler.doRetry(dropReplica, 0, retryable));
    }
}
