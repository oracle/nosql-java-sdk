/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.nosql.driver.ops.ListTablesRequest;

import org.junit.Before;
import org.junit.Test;

public class InternalsTest extends ProxyTestBase {

    @Override
    protected void perTestHandleConfig(NoSQLHandleConfig config) {
        config.setNumThreads(8);
    }

    @Before
    @Override
    /*
     * Override the default beforeTest() so we do not make
     * any requests at all to the server before the actual test
     * is run. The default runs a ListTables request.
     */
    public void beforeTest() throws Exception {
        super.beforeTest();
        handle = getHandle(endpoint);
        /* do NOT list tables here */
    }

    @Test
    public void serialSetupTest() {
        /*
         * Start N threads. Make them all run at the same time.
         * Each thread will issue requests. Verify that none of them
         * throw UnsupportedProtocolException. This verifies that the
         * logic inside Client properly manages setting the serial version
         * on the first request(s) that may happen in parallel.
         *
         * This test really only does anything if the server it's running
         * against is an older version. But it's ok to run with the current
         * server version anyway.
         */
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger(0);
        final int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        for (int x=0; x<numThreads; x++) {
            threads[x] = new Thread(() -> {
                try { latch.await(); } catch (Exception e) {}
                runRequests(errors);
            });
            threads[x].start();
        }

        latch.countDown();

        /* wait for threads to finish */
        for(int x=0; x<numThreads; x++) {
            try { threads[x].join(); } catch (Exception e) {}
        }
        if (errors.get() > 0) {
            fail("Got " + errors.get() + " errors");
        }
    }

    private void runRequests(AtomicInteger errors) {
        ListTablesRequest req = new ListTablesRequest()
              .setLimit(1);
        try {
            for (int x=0; x<5; x++) {
                handle.listTables(req);
            }
        } catch (Exception e) {
            System.out.println("Got exception: " + e);
            errors.incrementAndGet();
        }
    }
}
