/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableRequest;

public class SignatureProviderTest extends DriverTestBase {

    @Test
    public void testGetAuthorizationString()
        throws Exception {

        String pemKey = getPEMKey(genKeyPair().getPrivate());
        SignatureProvider provider = new SignatureProvider(
            SimpleProfileProvider.builder()
            .tenantId("ocid1.tenancy.oc1..tenancy")
            .userId("ocid1.user.oc1..user")
            .fingerprint("fingerprint")
            .privateKeySupplier(new PrivateKeyStringSupplier(pemKey))
            .build());

        /* which request not matter */
        Request request = new TableRequest();

        try {
            provider.getAuthorizationString(request);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "service host");
        }
        provider.prepare(new NoSQLHandleConfig("http://test"));
        String authzString = provider.getAuthorizationString(request);

        /* default cache duration about 5 mins, string should be the same */
        assertEquals(authzString, provider.getAuthorizationString(request));
    }

    @Test
    public void testRefresh()
        throws Exception {

        String pemKey = getPEMKey(genKeyPair().getPrivate());
        SignatureProvider provider = new SignatureProvider(
            SimpleProfileProvider.builder()
            .tenantId("ocid1.tenancy.oc1..tenancy")
            .userId("ocid1.user.oc1..user")
            .fingerprint("fingerprint")
            .privateKeySupplier(new PrivateKeyStringSupplier(pemKey))
            .build(),
            1 /* duration 1 seconds */,
            10 /* 10 ms */);
        SigRefresh refreshCallback = new SigRefresh();
        provider.setOnSignatureRefresh(refreshCallback);
        provider.prepare(new NoSQLHandleConfig("http://test"));

        Request request = new TableRequest();
        String authzString = provider.getAuthorizationString(request);
        Thread.sleep(2000);

        /* the new signature string should be cached */
        assertNotEquals(authzString, provider.getAuthorizationString(request));
        /*
         * refresh should have happened twice. Be flexible and only check >= 1
         */
        assertTrue(refreshCallback.getNumCalls() >= 1);

        /*
         * Exercise concurrent refresh schedule. The refresh might be scheduled
         * by NoSQLHandle.getAuthorizationString or the refresh task itself.
         * Start two threads to call the common getSignatureDetailsInternal
         * simultaneously that would schedule a refresh to simulate this case.
         */
        CountDownLatch startFlag = new CountDownLatch(1);
        Set<Thread> threads = new HashSet<Thread>();
        for (int i = 0; i < 2; i++) {
            Thread exerciseTask = new Thread() {

                @Override
                public void run() {
                    try {
                        startFlag.await();
                        assertNotNull(
                            provider.getSignatureDetailsInternal(
                                    false, null, null, null /* content */));
                    } catch (InterruptedException e) {
                    }
                }
            };
            exerciseTask.start();
            threads.add(exerciseTask);
        }

        startFlag.countDown();
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    public void testRegionProvider()
        throws Exception {

        File key = new File(generatePrivateKeyFile("key.pem", null));
        SignatureProvider provider = new SignatureProvider(
            "ocid1.tenancy.oc1..tenancy",
            "ocid1.user.oc1..user",
            "fingerprint",
            key,
            null,
            (Region)null);

        try {
            new NoSQLHandleConfig(provider);
            fail("no region");
        } catch (IllegalArgumentException iae) {
        }

        provider = new SignatureProvider(
            "ocid1.tenancy.oc1..tenancy",
            "ocid1.user.oc1..user",
            "fingerprint",
            key,
            null,
            Region.AP_MUMBAI_1);
        NoSQLHandleConfig config = new NoSQLHandleConfig(provider);
        URL serviceURL = config.getServiceURL();
        assertEquals(serviceURL.toString(),
                     Region.AP_MUMBAI_1.endpoint() + ":443/");

        try {
            new NoSQLHandleConfig(Region.AP_MELBOURNE_1, provider);
            fail("mismatch region");
        } catch (IllegalArgumentException iae) {
        }

        config = new NoSQLHandleConfig(Region.AP_MUMBAI_1, provider);
        serviceURL = config.getServiceURL();
        assertEquals(serviceURL.toString(),
                     Region.AP_MUMBAI_1.endpoint() + ":443/");
    }

    private KeyPair genKeyPair()
        throws Exception {

        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.genKeyPair();
    }

    private String getPEMKey(PrivateKey key) {
        StringBuilder sb = new StringBuilder();
        sb.append("-----BEGIN PRIVATE KEY-----\n")
        .append(Base64.getEncoder().encodeToString(key.getEncoded()))
        .append("\n-----END PRIVATE KEY-----");
        return sb.toString();
    }

    private class SigRefresh implements SignatureProvider.OnSignatureRefresh {
        private int numCalls;

        /*
         * Attempt to refresh the server's authentication and authorization
         * information for a new signature.
         */
        @Override
        public void refresh(long refreshMs) {
            ++numCalls;
        }

        private int getNumCalls() {
            return numCalls;
        }
    }
}
