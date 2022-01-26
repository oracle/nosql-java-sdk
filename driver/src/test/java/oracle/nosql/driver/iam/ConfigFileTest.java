/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.OCIConfigFileReader.OCIConfigFile;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigFileTest extends DriverTestBase {

    @Before
    public void setUp() {
        clearTestDirectory();
    }

    @After
    public void tearDown() {
        clearTestDirectory();
    }

    @Test
    public void testParseProfile()
        throws Exception {

        File config = new File(getTestDir(), "parse_profile");
        PrintWriter writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println("key=value");
        writer.println(" ");
        writer.println("key2= value2");
        writer.println("   # ignore line");
        writer.println(" key3 =value3");
        writer.println("   # ignore line");
        writer.println("key4 = value4     ");
        writer.println("key5 ==val=ue=");
        writer.println("[key6=value6");
        writer.println(" ");
        writer.println("[ PROFILE1 ]");
        writer.println("key=new value");
        writer.println(" ");
        writer.println("[PROFILE2 ]");
        writer.println("key = value=foobar");
        writer.println("key2=nota#comment");
        writer.println("  ");
        writer.close();

        /* parse default profile */
        OCIConfigFile cFile = OCIConfigFileReader.parse(config.getPath());
        assertEquals("value", cFile.get("key"));
        assertEquals("value2", cFile.get("key2"));
        assertEquals("value3", cFile.get("key3"));
        assertEquals("value4", cFile.get("key4"));
        assertEquals("=val=ue=", cFile.get("key5"));
        assertEquals("value6", cFile.get("[key6"));

        /* parse override profile */
        cFile = OCIConfigFileReader.parse(config.getPath(), "PROFILE1");
        assertEquals("new value", cFile.get("key"));
        assertEquals("value2", cFile.get("key2"));

        cFile = OCIConfigFileReader.parse(config.getPath(), "PROFILE2");
        assertEquals("value=foobar", cFile.get("key"));
        assertEquals("nota#comment", cFile.get("key2"));

        /* parse non-existent profile */
        try {
            OCIConfigFileReader.parse(config.getPath(), "non-existent");
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "No profile");
        }
    }

    @Test
    public void testErrorConfigFile()
        throws Exception {

        /* parse non-existent file */
        try {
            OCIConfigFileReader.parse("nonexistent_file");
            fail("expected");
        } catch (FileNotFoundException e) {
            assertThat(e.getMessage(), "such file");
        }

        /* no default profile */
        File nodefault = new File(getTestDir(), "no_default_profile");
        PrintWriter writer = new PrintWriter(nodefault);
        writer.println("[USER]");
        writer.println("tenancy=default_tenancy");
        writer.println("user=default_user");
        writer.println("fingerprint=default_fingerprint");
        writer.println("key_file=default_key_file");
        writer.close();

        OCIConfigFile cFile = OCIConfigFileReader.parse(nodefault.getPath(),
                                                        "USER");
        assertEquals("default_user", cFile.get("user"));
        assertEquals("default_tenancy", cFile.get("tenancy"));
        assertNull(cFile.get("region"));

        /* no leading profile */
        File noleading = new File(getTestDir(), "no_leading_profile");
        writer = new PrintWriter(noleading);
        writer.println("foo=bar");
        writer.close();

        try {
            OCIConfigFileReader.parse(noleading.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "no profile specified");
        }

        /* line with no value */
        File novalue = new File(getTestDir(), "no_value_profile");
        writer = new PrintWriter(novalue);
        writer.println("[DEFAULT]");
        writer.println("foo");
        writer.close();

        try {
            OCIConfigFileReader.parse(novalue.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "Invalid line in OCI");
        }

        /* line with empty key */
        File nokey = new File(getTestDir(), "no_key_profile");
        writer = new PrintWriter(nokey);
        writer.println("[DEFAULT]");
        writer.println(" =bar");
        writer.close();

        try {
            OCIConfigFileReader.parse(nokey.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "Invalid line in OCI");
        }

        /* empty profile name */
        File noprofileName = new File(getTestDir(), "no_profile_name");
        writer = new PrintWriter(noprofileName);
        writer.println("[        ]");
        writer.close();

        try {
            OCIConfigFileReader.parse(noprofileName.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "Invalid line in OCI");
        }
    }

    @Test
    public void testReadPrivateKey()
        throws Exception {

        File key = new File(generatePrivateKeyFile("api-key.pem", null));
        PrivateKeyProvider keyProvider = new PrivateKeyProvider(
            new FileInputStream(key), null);
        assertNotNull(keyProvider.getKey());

        char[] ph = "123456".toCharArray();
        key = new File(generatePrivateKeyFile("enc-api-key.pem", ph));
        keyProvider = new PrivateKeyProvider(new FileInputStream(key), ph);
        assertNotNull(keyProvider.getKey());
    }

    @Test
    public void testRegionProvider()
        throws Exception {

        File key = new File(generatePrivateKeyFile("api-key.pem", null));

        File config = new File(getTestDir(), "region_profile");
        PrintWriter writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println("tenancy=default_tenancy");
        writer.println("user=default_user");
        writer.println("fingerprint=default_fingerprint");
        writer.println("key_file=" + key.getAbsolutePath());
        writer.println("region=ap-melbourne-1");
        writer.close();

        SignatureProvider provider =
            new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        assertEquals(provider.getRegion(), Region.AP_MELBOURNE_1);

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(provider);
        assertEquals(hconfig.getServiceURL().toString(),
                     Region.AP_MELBOURNE_1.endpoint() + ":443/");

        config = new File(getTestDir(), "noregion_profile");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println("tenancy=default_tenancy");
        writer.println("user=default_user");
        writer.println("fingerprint=default_fingerprint");
        writer.println("key_file=" + key.getAbsolutePath());
        writer.close();

        provider = new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        hconfig = new NoSQLHandleConfig(Region.ME_JEDDAH_1, provider);
        assertEquals(hconfig.getServiceURL().toString(),
                     Region.ME_JEDDAH_1.endpoint() + ":443/");

        config = new File(getTestDir(), "mismatchregion_profile");
        writer = new PrintWriter(config);
        writer.println("[DEFAULT]");
        writer.println("tenancy=default_tenancy");
        writer.println("user=default_user");
        writer.println("fingerprint=default_fingerprint");
        writer.println("key_file=" + key.getAbsolutePath());
        writer.println("region=ap-melbourne-1");
        writer.close();

        provider = new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        try {
            new NoSQLHandleConfig(Region.ME_JEDDAH_1, provider);
            fail("mismatch region");
        } catch (IllegalArgumentException iae) {
        }
    }
}
