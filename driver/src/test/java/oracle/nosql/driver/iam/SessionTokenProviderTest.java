/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.OCIConfigFileReader.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;
import java.util.Scanner;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.Region;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SessionTokenProviderTest extends DriverTestBase {
    @Before
    public void setUp() {
        clearTestDirectory();
    }

    @After
    public void tearDown() {
        clearTestDirectory();
    }

    @Test
    public void testSessionTokenProvider()
        throws Exception {

        String key = "pseudo-key";
        File keyFile = new File(getTestDir(), "key");
        try (PrintWriter writer = new PrintWriter(keyFile)) {
            writer.println(key);
        }
        String token = "pseudo-token";
        File tokenFile = new File(getTestDir(), "token");
        try (PrintWriter writer = new PrintWriter(tokenFile)) {
            writer.println(token);
        }
        String tenantId = "ocid1.tenancy.oc1..tenancy";
        Region region = Region.US_ASHBURN_1;

        File config = new File(getTestDir(), "normalconfig");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
            writer.println(SESSION_TOKEN_FILE_PROP + "=" +
                           tokenFile.getAbsolutePath());
            writer.println(FINGERPRINT_PROP + "=fingerprint");
            writer.println(KEY_FILE_PROP + "=" + keyFile.getAbsolutePath());
            writer.println(REGION_PROP + "=" + region.getRegionId());

        }

        SessionTokenProvider provider =
            new SessionTokenProvider(config.getPath(), DEFAULT_PROFILE_NAME);
        assertEquals("ST$" + token, provider.getKeyId());
        assertEquals(tenantId, provider.getTenantId());
        assertEquals(region, provider.getRegion());

        try (Scanner scanner = new Scanner(provider.getPrivateKey())) {
            assertEquals(key, scanner.next());
        }

        keyFile = new File(generatePrivateKeyFile("api-key.pem", null));
        config = new File(getTestDir(), "keyconfig");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
            writer.println(SESSION_TOKEN_FILE_PROP + "=" +
                tokenFile.getAbsolutePath());
            writer.println(FINGERPRINT_PROP + "=fingerprint");
            writer.println(KEY_FILE_PROP + "=" + keyFile.getAbsolutePath());
            writer.println(REGION_PROP + "=" + region.getRegionId());

        }
        SignatureProvider sigProvider = SignatureProvider
            .createWithSessionToken(config.getPath(), DEFAULT_PROFILE_NAME);
        assertEquals(region, sigProvider.getRegion());

        /* non-existent key in config */
        config = new File(getTestDir(), "nokeyconfig");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
            writer.println(FINGERPRINT_PROP + "=fingerprint");
            writer.println(KEY_FILE_PROP + "=nonexistennt");
            writer.println(SESSION_TOKEN_FILE_PROP + "=" +
                tokenFile.getAbsolutePath());
        }

        try {
            provider = new SessionTokenProvider(
                config.getPath(), DEFAULT_PROFILE_NAME);
            provider.getPrivateKey();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "not find private key");
        }

        /* non tenant id in config */
        config = new File(getTestDir(), "notenantconfig");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println(USER_PROP + "=ocid1.user.oc1..user");
            writer.println(FINGERPRINT_PROP + "=fingerprint");
            writer.println(KEY_FILE_PROP + "=" + keyFile.getAbsolutePath());
            writer.println(SESSION_TOKEN_FILE_PROP + "=" +
                tokenFile.getAbsolutePath());
        }

        try {
            provider = new SessionTokenProvider(config.getPath(),
                DEFAULT_PROFILE_NAME);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), TENANCY_PROP + " is missing");
        }
    }
}
