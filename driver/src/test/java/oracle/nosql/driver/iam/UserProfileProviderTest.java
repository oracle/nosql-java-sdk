/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static oracle.nosql.driver.iam.OCIConfigFileProvider.*;

import java.io.File;
import java.io.PrintWriter;
import java.util.Scanner;

import oracle.nosql.driver.DriverTestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserProfileProviderTest extends DriverTestBase {

    @Before
    public void setUp() {
        clearTestDirectory();
    }

    @After
    public void tearDown() {
        clearTestDirectory();
    }

    @Test
    public void testSimpleProfileProvider() {
        SimpleProfileProvider provider = SimpleProfileProvider.builder()
            .tenantId("ocid1.tenancy.oc1..tenancy")
            .userId("ocid1.user.oc1..user")
            .fingerprint("fingerprint")
            .privateKeySupplier(new PrivateKeyStringSupplier("pseudo"))
            .build();

        assertEquals(provider.getFingerprint(), "fingerprint");
        assertEquals(provider.getKeyId(),
                     Utils.createKeyId("ocid1.tenancy.oc1..tenancy",
                                       "ocid1.user.oc1..user",
                                       "fingerprint"));
        try (Scanner scanner = new Scanner(provider.getPrivateKey())) {
            String key = scanner.next();
            assertEquals(key, "pseudo");
        }

        /* malformatted tenant id*/
        provider = SimpleProfileProvider.builder()
            .tenantId("malformatted")
            .userId("userid")
            .build();
        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "does not match");
        }

        /* no tenant id */
        provider = SimpleProfileProvider.builder()
            .userId("malformatted")
            .build();
        try {
            provider.getKeyId();
            fail("expected");
        } catch (NullPointerException npe) {
            assertThat(npe.getMessage(), "missing tenantId");
        }

        /* malformatted user id */
        provider = SimpleProfileProvider.builder()
            .tenantId("ocid1.tenancy.oc1..tenancy")
            .userId("malformatted")
            .build();
        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "does not match");
        }

        /* no user id */
        provider = SimpleProfileProvider.builder()
            .tenantId("ocid1.tenancy.oc1..tenancy")
            .build();
        try {
            provider.getKeyId();
            fail("expected");
        } catch (NullPointerException npe) {
            assertThat(npe.getMessage(), "missing userId");
        }
    }

    @Test
    public void testFileProfileProvider()
        throws Exception {

        File key = new File(getTestDir(), "key");
        PrintWriter writer = new PrintWriter(key);
        writer.println("pseudo");
        writer.close();

        File config = new File(getTestDir(), "normalconfig");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
        writer.println(USER_PROP + "=ocid1.user.oc1..user");
        writer.println(FINGERPRINT_PROP + "=fingerprint");
        writer.println(KEY_FILE_PROP + "=" + key.getAbsolutePath());
        writer.close();

        OCIConfigFileProvider provider =
            new OCIConfigFileProvider(config.getPath(), DEFAULT_PROFILE_NAME);
        assertEquals(provider.getFingerprint(), "fingerprint");
        assertEquals(provider.getKeyId(),
                     Utils.createKeyId("ocid1.tenancy.oc1..tenancy",
                                       "ocid1.user.oc1..user",
                                       "fingerprint"));
        try (Scanner scanner = new Scanner(provider.getPrivateKey())) {
            String value = scanner.next();
            assertEquals(value, "pseudo");
        }

        /* non-existent key in config */
        config = new File(getTestDir(), "nokeyconfig");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
        writer.println(USER_PROP + "=ocid1.user.oc1..user");
        writer.println(FINGERPRINT_PROP + "=fingerprint");
        writer.println(KEY_FILE_PROP + "=nonexistennt");
        writer.close();

        try {
            provider = new OCIConfigFileProvider(config.getPath(),
                                                 DEFAULT_PROFILE_NAME);
            provider.getPrivateKey();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "not find private key");
        }

        /* non tenant id in config */
        config = new File(getTestDir(), "notenantconfig");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println(USER_PROP + "=ocid1.user.oc1..user");
        writer.println(FINGERPRINT_PROP + "=fingerprint");
        writer.println(KEY_FILE_PROP + "=" + key.getAbsolutePath());
        writer.close();

        try {
            provider = new OCIConfigFileProvider(config.getPath(),
                                                 DEFAULT_PROFILE_NAME);
            fail("expected");
        } catch (NullPointerException npe) {
            assertThat(npe.getMessage(), "missing " + TENANCY_PROP);
        }

        /* non user id in config */
        config = new File(getTestDir(), "nouserconfig");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
        writer.println(FINGERPRINT_PROP + "=fingerprint");
        writer.println(KEY_FILE_PROP + "=" + key.getAbsolutePath());
        writer.close();

        try {
            provider = new OCIConfigFileProvider(config.getPath(),
                                                 DEFAULT_PROFILE_NAME);
            fail("expected");
        } catch (NullPointerException npe) {
            assertThat(npe.getMessage(), "missing " + USER_PROP);
        }

        /* non fingerprint in config */
        config = new File(getTestDir(), "nofgconfig");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println(TENANCY_PROP + "=ocid1.tenancy.oc1..tenancy");
        writer.println(USER_PROP + "=ocid1.user.oc1..user");
        writer.println(KEY_FILE_PROP + "=" + key.getAbsolutePath());
        writer.close();

        try {
            provider = new OCIConfigFileProvider(config.getPath(),
                                                 DEFAULT_PROFILE_NAME);
            fail("expected");
        } catch (NullPointerException npe) {
            assertThat(npe.getMessage(), "missing " + FINGERPRINT_PROP);
        }
    }
}
