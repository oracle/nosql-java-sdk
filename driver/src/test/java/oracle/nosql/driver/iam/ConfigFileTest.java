/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.getIAMURL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.OCIConfigFileReader.OCIConfigFile;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

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
        try (PrintWriter writer = new PrintWriter(config)) {
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
        }

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
        try (PrintWriter writer = new PrintWriter(nodefault)) {
            writer.println("[USER]");
            writer.println("tenancy=default_tenancy");
            writer.println("user=default_user");
            writer.println("fingerprint=default_fingerprint");
            writer.println("key_file=default_key_file");
        }

        OCIConfigFile cFile = OCIConfigFileReader.parse(nodefault.getPath(),
                                                        "USER");
        assertEquals("default_user", cFile.get("user"));
        assertEquals("default_tenancy", cFile.get("tenancy"));
        assertNull(cFile.get("region"));

        /* no leading profile */
        File noleading = new File(getTestDir(), "no_leading_profile");
        try (PrintWriter writer = new PrintWriter(noleading)) {
            writer.println("foo=bar");
        }

        try {
            OCIConfigFileReader.parse(noleading.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "no profile specified");
        }

        /* line with no value */
        File novalue = new File(getTestDir(), "no_value_profile");
        try (PrintWriter writer = new PrintWriter(novalue)) {
            writer.println("[DEFAULT]");
            writer.println("foo");
        }

        try {
            OCIConfigFileReader.parse(novalue.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "Invalid line in OCI");
        }

        /* line with empty key */
        File nokey = new File(getTestDir(), "no_key_profile");
        try (PrintWriter writer = new PrintWriter(nokey)) {
            writer.println("[DEFAULT]");
            writer.println(" =bar");
        }

        try {
            OCIConfigFileReader.parse(nokey.getPath());
            fail("expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), "Invalid line in OCI");
        }

        /* empty profile name */
        File noprofileName = new File(getTestDir(), "no_profile_name");
        try (PrintWriter writer = new PrintWriter(noprofileName)) {
            writer.println("[        ]");
        }

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
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println("tenancy=default_tenancy");
            writer.println("user=default_user");
            writer.println("fingerprint=default_fingerprint");
            writer.println("key_file=" + key.getAbsolutePath());
            writer.println("region=ap-melbourne-1");
        }

        SignatureProvider provider =
            new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        assertEquals(provider.getRegion(), Region.AP_MELBOURNE_1);

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(provider);
        assertEquals(hconfig.getServiceURL().toString(),
                     Region.AP_MELBOURNE_1.endpoint() + ":443/");

        config = new File(getTestDir(), "noregion_profile");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println("tenancy=default_tenancy");
            writer.println("user=default_user");
            writer.println("fingerprint=default_fingerprint");
            writer.println("key_file=" + key.getAbsolutePath());
        }

        provider = new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        hconfig = new NoSQLHandleConfig(Region.ME_JEDDAH_1, provider);
        assertEquals(hconfig.getServiceURL().toString(),
                     Region.ME_JEDDAH_1.endpoint() + ":443/");

        config = new File(getTestDir(), "mismatchregion_profile");
        try (PrintWriter writer = new PrintWriter(config)) {
            writer.println("[DEFAULT]");
            writer.println("tenancy=default_tenancy");
            writer.println("user=default_user");
            writer.println("fingerprint=default_fingerprint");
            writer.println("key_file=" + key.getAbsolutePath());
            writer.println("region=ap-melbourne-1");
        }

        provider = new SignatureProvider(config.getAbsolutePath(), "DEFAULT");
        try {
            new NoSQLHandleConfig(Region.ME_JEDDAH_1, provider);
            fail("mismatch region");
        } catch (IllegalArgumentException iae) {
        }
    }

    private MapValue getRealm(ArrayValue realms, String realm) {
        for (FieldValue fv : realms) {
            MapValue mv = fv.asMap();
            String name = mv.get("name").asString().getValue();
            if (realm.compareTo(name) == 0) {
                return mv;
            }
        }
        return null;
    }

    private String nosqlEndpoint(ArrayValue realms, MapValue mv) {
        String realm = mv.get("realm").asString().getValue();
        MapValue realmVals = getRealm(realms, realm);
        if (realmVals == null) {
            return null;
        }
        String prefix = realmVals.get("epprefix").asString().getValue();
        String suffix = realmVals.get("epsuffix").asString().getValue();
        String regionId = mv.get("name").asString().getValue();
        return "https://" + prefix + regionId + suffix;
    }

    private String authEndpoint(ArrayValue realms, MapValue mv) {
        String realm = mv.get("realm").asString().getValue();
        MapValue realmVals = getRealm(realms, realm);
        if (realmVals == null) {
            return null;
        }
        String prefix = realmVals.get("authprefix").asString().getValue();
        String suffix = realmVals.get("authsuffix").asString().getValue();
        String regionId = mv.get("name").asString().getValue();
        return "https://" + prefix + regionId + suffix;
    }


    /*
     * To execute this specific test, given a regions.json file:
     *
     * mvn -Ptest-local -DargLine="-Dtest.regionsfile=<path>/regions.json" \
     *     -Dtest=oracle.nosql.driver.iam.ConfigFileTest test
     */
    @Test
    public void testAllRegionCodes() {
        /* only execute if we're supplied a path to regions.json file */
        String jsonPath = System.getProperty("test.regionsfile");
        assumeTrue("Skipping region codes test: no regionsfile given",
            (jsonPath != null && !jsonPath.isEmpty()));
        MapValue jsonMap = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(jsonPath);
            jsonMap = JsonUtils.createValueFromJson(fis, null).asMap();
        } catch (Exception e) {
            fail("Can't read regions json file '" + jsonPath + "': " + e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (Exception e) {}
            }
        }

        /*
         * Regions json has two fields: "realms" and "regions":
         *  "realms": [
         *      {
         *         "name": "oc1",
         *         "epprefix": "nosql.",
         *         "epsuffix": ".oci.oraclecloud.com",
         *         "authprefix": "auth.",
         *         "authsuffix": ".oraclecloud.com"
         *      },
         *      ...]
         *
         *  "regions": [
         *      {"tlc": "jnb", "realm": "oc1", "name": "af-johannesburg-1"},
         *      {"tlc": "yny", "realm": "oc1", "name": "ap-chuncheon-1"},
         *      {"tlc": "nja", "realm": "oc8", "name": "ap-chiyoda-1"},
         *      ...]
         *
         * Walk all regions, and verify endpoints for each.
         */
        ArrayValue realms = jsonMap.get("realms").asArray();
        StringBuilder errs = new StringBuilder();
        ArrayValue regions = jsonMap.get("regions").asArray();
        for (FieldValue fv : regions) {
            MapValue mv = fv.asMap();
            String regionId = mv.get("name").asString().getValue();
            Region r = Region.fromRegionId(regionId);
            if (r == null) {
                errs.append(" Missing region '").append(regionId).append("'\n");
            } else {
                String endpoint = r.endpoint();
                String expEndpoint = nosqlEndpoint(realms, mv);
                if (expEndpoint.compareTo(endpoint) != 0) {
                    errs.append(" Wrong nosql endpoint for region '")
                        .append(regionId).append("':\n")
                        .append("  expected=").append(expEndpoint).append("\n")
                        .append("  observed=").append(endpoint).append("\n");
                }
            }
            String expAuthURL = authEndpoint(realms, mv);

            String authURL = getIAMURL(regionId);
            if (authURL == null || expAuthURL.compareTo(authURL) != 0) {
                errs.append(" Wrong auth endpoint for region '")
                    .append(regionId).append("':\n")
                    .append("  expected=").append(expAuthURL).append("\n")
                    .append("  observed=").append(authURL).append("\n");
            }
            String regionCode = mv.get("tlc").asString().getValue();
            authURL = getIAMURL(regionCode);
            if (authURL == null ||expAuthURL.compareTo(authURL) != 0) {
                errs.append(" Wrong auth endpoint for region code '")
                    .append(regionCode).append("':\n")
                    .append("  expected=").append(expAuthURL).append("\n")
                    .append("  observed=").append(authURL).append("\n");
            }
        }
        if (errs.length() > 0) {
            fail("\nErrors found in regions:\n" + errs.toString());
        }
    }
}
