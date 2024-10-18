/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.Region;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RegionTest extends DriverTestBase {
    private static final String basePath = getResourcesDir();
    private static final String regionConfigName = "regions-config.json";
    private static final String envVarRegion =
        "{" +
        " \"regionKey\": \"abc\", " +
        " \"realmKey\": \"oc99\", " +
        " \"regionIdentifier\": \"us-nowhere-1\", " +
        " \"realmDomainComponent\": \"nocloud.com\" " +
        "}";

    /* missing realm key */
    private static final String badRegion =
        "{" +
        " \"regionKey\": \"abc\", " +
        " \"regionIdentifier\": \"us-nowhere-1\", " +
        " \"realmDomainComponent\": \"nocloud.com\" " +
        "}";

    @BeforeClass
    public static void staticSetup() {
        System.setProperty("OCI_REGION_METADATA", envVarRegion);
    }

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
        assertNull(Region.fromRegionId("notARegion"));
        assertNull(Region.fromRegionId("af-newregion-1"));

        /* load a fake regions-config.json file */
        Region.readRegionConfigFile(basePath + "/" + regionConfigName);
        /* it should exist now */
        assertNotNull(Region.fromRegionId("af-newregion-1"));

        /* good, existing region */
        assertNotNull(Region.fromRegionId("us-ashburn-1"));

        /* re-read env var, as previous tests may have set one-time-use flag */
        Region.readEnvVar();

        /* this was added by env var, see static init above */
        assertNotNull(Region.fromRegionId("us-nowhere-1"));

        /* bad region in env */
        System.setProperty("OCI_REGION_METADATA", badRegion);
        try {
            Region.readEnvVar();
            fail("Should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("issing field realmKey"));
        }
    }
}
