/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Cloud service only.
 * <p>
 * The class contains the regions in the Oracle Cloud Infrastructure at the
 * time of this release. The Oracle NoSQL Database Cloud Service is not
 * available in all of these regions. For a definitive list of regions
 * in which the Oracle NoSQL Database Cloud Service is available see
 * <a href="https://www.oracle.com/cloud/data-regions.html">Data Regions
 * for Platform and Infrastructure Services</a>.
 * <p>
 * A Region may be provided to
 * {@link NoSQLHandleConfig#NoSQLHandleConfig(Region,AuthorizationProvider)}
 * to configure a handle to communicate in a specific Region.
 * <p>
 * The string-based endpoints associated with regions for the Oracle NoSQL
 * Database Cloud Service are of the format
 * <pre>    https://nosql.{region}.oci.{secondLevelDomain} </pre>
 * Examples of known second level domains include:
 * <ul>
 * <li>oraclecloud.com</li>
 * <li>oraclegovcloud.com</li>
 * <li>oraclegovcloud.uk</li>
 * </ul>
 * For example, this is a valid endpoint for the Oracle NoSQL Database Cloud
 * Service in the U.S. East region
 * <pre>    nosql.us-ashburn-1.oci.oraclecloud.com </pre>
 * If the Oracle NoSQL Database Cloud Service becomes available in a region
 * not listed here it is possible to connect to that region using the endpoint
 * and
 * {@link NoSQLHandleConfig#NoSQLHandleConfig(String,AuthorizationProvider)}
 * <p>
 * For more information about Oracle Cloud Infrastructure regions see
 * <a href="https://docs.cloud.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm">Regions and Availability Domains</a>.
 */
public class Region {
    private static final Map<String, Region> OC1_REGIONS = new HashMap<>();
    private static final Map<String, Region> GOV_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC4_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC8_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC9_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC10_REGIONS = new HashMap<>();

    /* OC1 */
    public static final Region AF_JOHANNESBURG_1 = new Region("af-johannesburg-1");

    public static final Region AP_CHUNCHEON_1 = new Region("ap-chuncheon-1");
    public static final Region AP_HYDERABAD_1 = new Region("ap-hyderabad-1");
    public static final Region AP_MELBOURNE_1 = new Region("ap-melbourne-1");
    public static final Region AP_MUMBAI_1 = new Region("ap-mumbai-1");
    public static final Region AP_OSAKA_1 = new Region("ap-osaka-1");
    public static final Region AP_SINGAPORE_1 = new Region("ap-singapore-1");
    public static final Region AP_SEOUL_1 = new Region("ap-seoul-1");
    public static final Region AP_SYDNEY_1 = new Region("ap-sydney-1");
    public static final Region AP_TOKYO_1 = new Region("ap-tokyo-1");

    public static final Region UK_CARDIFF_1 = new Region("uk-cardiff-1");
    public static final Region UK_LONDON_1 = new Region("uk-london-1");

    public static final Region EU_AMSTERDAM_1 = new Region("eu-amsterdam-1");
    public static final Region EU_FRANKFURT_1 = new Region("eu-frankfurt-1");
    public static final Region EU_MARSEILLE_1 = new Region("eu-marseille-1");
    public static final Region EU_MILAN_1 = new Region("eu-milan-1");
    public static final Region EU_STOCKHOLM_1 = new Region("eu-stockholm-1");
    public static final Region EU_ZURICH_1 = new Region("eu-zurich-1");

    public static final Region ME_ABUDHABI_1 = new Region("me-abudhabi-1");
    public static final Region ME_DUBAI_1 = new Region("me-dubai-1");
    public static final Region ME_JEDDAH_1 = new Region("me-jeddah-1");

    public static final Region IL_JERUSALEM_1 = new Region("il-jerusalem-1");

    public static final Region US_ASHBURN_1 = new Region("us-ashburn-1");
    public static final Region US_PHOENIX_1 = new Region("us-phoenix-1");
    public static final Region US_SANJOSE_1 = new Region("us-sanjose-1");

    public static final Region CA_MONTREAL_1 = new Region("ca-montreal-1");
    public static final Region CA_TORONTO_1 = new Region("ca-toronto-1");

    public static final Region SA_SAOPAULO_1 = new Region("sa-saopaulo-1");
    public static final Region SA_SANTIAGO_1 = new Region("sa-santiago-1");
    public static final Region SA_VINHEDO_1 = new Region("sa-vinhedo-1");

    /* OC2 */
    public static final Region US_LANGLEY_1 = new Region("us-langley-1");
    public static final Region US_LUKE_1 = new Region("us-luke-1");

    /* OC3 */
    public static final Region US_GOV_ASHBURN_1 = new Region("us-gov-ashburn-1");
    public static final Region US_GOV_CHICAGO_1 = new Region("us-gov-chicago-1");
    public static final Region US_GOV_PHOENIX_1 = new Region("us-gov-phoenix-1");

    /* OC4 */
    public static final Region UK_GOV_LONDON_1 = new Region("uk-gov-london-1");
    public static final Region UK_GOV_CARDIFF_1 = new Region("uk-gov-london-1");

    /* OC8 */
    public static final Region AP_CHIYODA_1 = new Region("ap-chiyoda-1");
    public static final Region AP_IBARAKI_1 = new Region("ap-ibaraki-1");

    /* OC9 */
    public static final Region ME_DCC_MUSCAT_1 = new Region("me-dcc-muscat-1");

    /* OC10 */
    public static final Region AP_DCC_CANBERRA_1 = new Region("ap-dcc-canberra-1");

    static {
        /* OC1 */
        /* AF */
        OC1_REGIONS.put(AF_JOHANNESBURG_1.getRegionId(), AF_JOHANNESBURG_1);

        /* APAC */
        OC1_REGIONS.put(AP_CHUNCHEON_1.getRegionId(), AP_CHUNCHEON_1);
        OC1_REGIONS.put(AP_HYDERABAD_1.getRegionId(), AP_HYDERABAD_1);
        OC1_REGIONS.put(AP_MELBOURNE_1.getRegionId(), AP_MELBOURNE_1);
        OC1_REGIONS.put(AP_MUMBAI_1.getRegionId(), AP_MUMBAI_1);
        OC1_REGIONS.put(AP_OSAKA_1.getRegionId(), AP_OSAKA_1);
        OC1_REGIONS.put(AP_SINGAPORE_1.getRegionId(), AP_SINGAPORE_1);
        OC1_REGIONS.put(AP_SEOUL_1.getRegionId(), AP_SEOUL_1);
        OC1_REGIONS.put(AP_SYDNEY_1.getRegionId(), AP_SYDNEY_1);
        OC1_REGIONS.put(AP_TOKYO_1.getRegionId(), AP_TOKYO_1);

        /* EMEA */
        OC1_REGIONS.put(UK_CARDIFF_1.getRegionId(), UK_CARDIFF_1);
        OC1_REGIONS.put(UK_LONDON_1.getRegionId(), UK_LONDON_1);

        OC1_REGIONS.put(EU_AMSTERDAM_1.getRegionId(), EU_AMSTERDAM_1);
        OC1_REGIONS.put(EU_FRANKFURT_1.getRegionId(), EU_FRANKFURT_1);
        OC1_REGIONS.put(EU_MARSEILLE_1.getRegionId(), EU_MARSEILLE_1);
        OC1_REGIONS.put(EU_MILAN_1.getRegionId(), EU_MILAN_1);
        OC1_REGIONS.put(EU_STOCKHOLM_1.getRegionId(), EU_STOCKHOLM_1);
        OC1_REGIONS.put(EU_ZURICH_1.getRegionId(), EU_ZURICH_1);

        OC1_REGIONS.put(ME_ABUDHABI_1.getRegionId(), ME_ABUDHABI_1);
        OC1_REGIONS.put(ME_DUBAI_1.getRegionId(), ME_DUBAI_1);
        OC1_REGIONS.put(ME_JEDDAH_1.getRegionId(), ME_JEDDAH_1);

        OC1_REGIONS.put(IL_JERUSALEM_1.getRegionId(), IL_JERUSALEM_1);

        /* LAD */
        OC1_REGIONS.put(SA_SAOPAULO_1.getRegionId(), SA_SAOPAULO_1);
        OC1_REGIONS.put(SA_SANTIAGO_1.getRegionId(), SA_SANTIAGO_1);
        OC1_REGIONS.put(SA_VINHEDO_1.getRegionId(), SA_SANTIAGO_1);

        /* North America */
        OC1_REGIONS.put(US_ASHBURN_1.getRegionId(), US_ASHBURN_1);
        OC1_REGIONS.put(US_PHOENIX_1.getRegionId(), US_PHOENIX_1);
        OC1_REGIONS.put(US_SANJOSE_1.getRegionId(), US_SANJOSE_1);

        OC1_REGIONS.put(CA_MONTREAL_1.getRegionId(), CA_MONTREAL_1);
        OC1_REGIONS.put(CA_TORONTO_1.getRegionId(), CA_TORONTO_1);

        /* OC2 */
        GOV_REGIONS.put(US_LANGLEY_1.getRegionId(), US_LANGLEY_1);
        GOV_REGIONS.put(US_LUKE_1.getRegionId(), US_LUKE_1);

        /* OC3 */
        GOV_REGIONS.put(US_GOV_ASHBURN_1.getRegionId(), US_GOV_ASHBURN_1);
        GOV_REGIONS.put(US_GOV_CHICAGO_1.getRegionId(), US_GOV_CHICAGO_1);
        GOV_REGIONS.put(US_GOV_PHOENIX_1.getRegionId(), US_GOV_PHOENIX_1);

        /* OC4 */
        OC4_REGIONS.put(UK_GOV_LONDON_1.getRegionId(), UK_GOV_LONDON_1);
        OC4_REGIONS.put(UK_GOV_CARDIFF_1.getRegionId(), UK_GOV_CARDIFF_1);

        /* OC8 */
        OC8_REGIONS.put(AP_CHIYODA_1.getRegionId(), AP_CHIYODA_1);
        OC8_REGIONS.put(AP_IBARAKI_1.getRegionId(), AP_IBARAKI_1);

        /* OC9 */
        OC9_REGIONS.put(ME_DCC_MUSCAT_1.getRegionId(), ME_DCC_MUSCAT_1);

        /* OC10 */
        OC10_REGIONS.put(AP_DCC_CANBERRA_1.getRegionId(), AP_DCC_CANBERRA_1);
    }

    private final static MessageFormat OC1_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud.com");
    private final static MessageFormat GOV_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclegovcloud.com");
    private final static MessageFormat OC4_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclegovcloud.uk");
    private final static MessageFormat OC8_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud8.com");
    private final static MessageFormat OC9_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud9.com");
    private final static MessageFormat OC10_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud10.com");

    private String regionId;

    private Region(String regionId) {
        this.regionId = regionId;
    }

    /**
     * Returns the NoSQL Database Cloud Service Endpoint for this region.
     * @return NoSQL Database Cloud Service Endpoint
     */
    public String endpoint() {
        if (isOC1Region(regionId)) {
            return OC1_EP_BASE.format(new Object[] { regionId });
        }
        if (isGovRegion(regionId)) {
            return GOV_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC4Region(regionId)) {
            return OC4_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC8Region(regionId)) {
            return OC8_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC9Region(regionId)) {
            return OC9_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC10Region(regionId)) {
            return OC10_EP_BASE.format(new Object[] { regionId });
        }
        throw new IllegalArgumentException(
            "Unable to find endpoint for unknwon region" + regionId);
    }

    /**
     * Returns the Region associated with the string value supplied,
     * or null if the string does not represent a known region.
     *
     * @param regionId the string value of the region
     * @return the Region or null if the string does not represent a
     * Region.
     */
    public static Region fromRegionId(String regionId) {
        if (regionId == null) {
            throw new IllegalArgumentException("Invalid region id " + regionId);
        }
        regionId = regionId.toLowerCase();
        Region region = OC1_REGIONS.get(regionId);
        if (region == null) {
            region = OC4_REGIONS.get(regionId);
        }
        if (region == null) {
            region = GOV_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC8_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC9_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC10_REGIONS.get(regionId);
        }

        return region;
    }

    /**
     * @hidden
     * Internal use only
     * @return the region id
     */
    public String getRegionId() {
        return regionId;
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC1Region(String regionId) {
        return (OC1_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isGovRegion(String regionId) {
        return (GOV_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC4Region(String regionId) {
        return (OC4_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC8Region(String regionId) {
        return (OC8_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC9Region(String regionId) {
        return (OC9_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC10Region(String regionId) {
        return (OC10_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC1Regions() {
        return OC1_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getGovRegions() {
        return GOV_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC4Regions() {
        return OC4_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC8Regions() {
        return OC8_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC9Regions() {
        return OC9_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC10Regions() {
        return OC10_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     */
    public interface RegionProvider {

        /**
         * @hidden
         * @return the Region to use for NoSQLHandle
         */
        Region getRegion();
    }
}
