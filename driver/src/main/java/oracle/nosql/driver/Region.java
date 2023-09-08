/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
    private static final Map<String, Region> OC5_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC8_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC9_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC10_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC14_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC16_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC17_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC19_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC20_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC22_REGIONS = new HashMap<>();
    private static final Map<String, Region> OC24_REGIONS = new HashMap<>();

    /* OC1 */
    /** JNB */
    public static final Region AF_JOHANNESBURG_1 = new Region("af-johannesburg-1");
    /** YNY */
    public static final Region AP_CHUNCHEON_1 = new Region("ap-chuncheon-1");
    /** HYD */
    public static final Region AP_HYDERABAD_1 = new Region("ap-hyderabad-1");
    /** MEL */
    public static final Region AP_MELBOURNE_1 = new Region("ap-melbourne-1");
    /** BOM */
    public static final Region AP_MUMBAI_1 = new Region("ap-mumbai-1");
    /** KIX */
    public static final Region AP_OSAKA_1 = new Region("ap-osaka-1");
    /** ICN */
    public static final Region AP_SINGAPORE_1 = new Region("ap-singapore-1");
    /** SIN */
    public static final Region AP_SEOUL_1 = new Region("ap-seoul-1");
    /** SYD */
    public static final Region AP_SYDNEY_1 = new Region("ap-sydney-1");
    /** NRT */
    public static final Region AP_TOKYO_1 = new Region("ap-tokyo-1");

    /** CWL */
    public static final Region UK_CARDIFF_1 = new Region("uk-cardiff-1");
    /** LHR */
    public static final Region UK_LONDON_1 = new Region("uk-london-1");

    /** AMS */
    public static final Region EU_AMSTERDAM_1 = new Region("eu-amsterdam-1");
    /** FRA */
    public static final Region EU_FRANKFURT_1 = new Region("eu-frankfurt-1");
    /** MAD */
    public static final Region EU_MADRID_1 = new Region("eu-madrid-1");
    /** MRS */
    public static final Region EU_MARSEILLE_1 = new Region("eu-marseille-1");
    /** LIN */
    public static final Region EU_MILAN_1 = new Region("eu-milan-1");
    /** CDG */
    public static final Region EU_PARIS_1 = new Region("eu-paris-1");
    /** ARN */
    public static final Region EU_STOCKHOLM_1 = new Region("eu-stockholm-1");
    /** ZRH */
    public static final Region EU_ZURICH_1 = new Region("eu-zurich-1");

    /** AUH */
    public static final Region ME_ABUDHABI_1 = new Region("me-abudhabi-1");
    /** DXB */
    public static final Region ME_DUBAI_1 = new Region("me-dubai-1");
    /** JED */
    public static final Region ME_JEDDAH_1 = new Region("me-jeddah-1");

    /** QRO */
    public static final Region MX_QUERETARO_1 = new Region("mx-queretaro-1");
    /** MTY */
    public static final Region MX_MONTERREY_1 = new Region("mx-monterrey-1");

    /** MTZ */
    public static final Region IL_JERUSALEM_1 = new Region("il-jerusalem-1");

    /** IAD */
    public static final Region US_ASHBURN_1 = new Region("us-ashburn-1");
    /** PHX */
    public static final Region US_PHOENIX_1 = new Region("us-phoenix-1");
    /** SJC */
    public static final Region US_SANJOSE_1 = new Region("us-sanjose-1");
    /** AGA */
    public static final Region US_SALTLAKE_2 = new Region("us-saltlake-2");
    /** ORD */
    public static final Region US_CHICAGO_1 = new Region("us-chicago-1");

    /** YUL */
    public static final Region CA_MONTREAL_1 = new Region("ca-montreal-1");
    /** YYZ */
    public static final Region CA_TORONTO_1 = new Region("ca-toronto-1");

    /** GRU */
    public static final Region SA_SAOPAULO_1 = new Region("sa-saopaulo-1");
    /** SCL */
    public static final Region SA_SANTIAGO_1 = new Region("sa-santiago-1");
    /** VAP */
    public static final Region SA_VALPARAISO_1 = new Region("sa-valparaiso-1");
    /** VCP */
    public static final Region SA_VINHEDO_1 = new Region("sa-vinhedo-1");

    /* OC2 */
    /** LFI */
    public static final Region US_LANGLEY_1 = new Region("us-langley-1");
    /** LUF */
    public static final Region US_LUKE_1 = new Region("us-luke-1");

    /* OC3 */
    /** RIC */
    public static final Region US_GOV_ASHBURN_1 = new Region("us-gov-ashburn-1");
    /** PIA */
    public static final Region US_GOV_CHICAGO_1 = new Region("us-gov-chicago-1");
    /** TUS */
    public static final Region US_GOV_PHOENIX_1 = new Region("us-gov-phoenix-1");

    /* OC4 */
    /** LTN */
    public static final Region UK_GOV_LONDON_1 = new Region("uk-gov-london-1");
    /** BRS */
    public static final Region UK_GOV_CARDIFF_1 = new Region("uk-gov-cardiff-1");

    /* OC5 */
    /** TIW */
    public static final Region US_TACOMA_1 = new Region("us-tacoma-1");

    /* OC8 */
    /** NJA */
    public static final Region AP_CHIYODA_1 = new Region("ap-chiyoda-1");
    /** UKB */
    public static final Region AP_IBARAKI_1 = new Region("ap-ibaraki-1");

    /* OC9 */
    /** MCT */
    public static final Region ME_DCC_MUSCAT_1 = new Region("me-dcc-muscat-1");

    /* OC10 */
    /** WGA */
    public static final Region AP_DCC_CANBERRA_1 = new Region("ap-dcc-canberra-1");

    /* OC14 */
    /** ORK */
    public static final Region AP_DCC_DUBLIN_1 = new Region("eu-dcc-dublin-1");
    /** SNN */
    public static final Region AP_DCC_DUBLIN_2 = new Region("eu-dcc-dublin-2");
    /** BGY */
    public static final Region AP_DCC_MILAN_1 = new Region("eu-dcc-milan-1");
    /** MXP */
    public static final Region AP_DCC_MILAN_2 = new Region("eu-dcc-milan-2");
    /** DUS */
    public static final Region AP_DCC_RATING_1 = new Region("eu-dcc-rating-1");
    /** DTM */
    public static final Region AP_DCC_RATING_2 = new Region("eu-dcc-rating-2");

    /* OC16 */
    /** SGU */
    public static final Region US_WESTJORDAN_1 = new Region("us-westjordan-1");

    /* OC17 */
    /** IFP */
    public static final Region US_DCC_PHOENIX_1 = new Region("us-dcc-phoenix-1");
    /** GCN */
    public static final Region US_DCC_PHOENIX_2 = new Region("us-dcc-phoenix-2");
    /** YUM */
    public static final Region US_DCC_PHOENIX_4 = new Region("us-dcc-phoenix-4");

    /* OC19 */
    /** STR */
    public static final Region EU_FRANKFURT_2 = new Region("eu-frankfurt-2");
    /** VLL */
    public static final Region EU_MADRID_2 = new Region("eu-madrid-2");

    /* OC20 */
    /** BEG */
    public static final Region EU_JOVANOVAC_1 = new Region("eu-jovanovac-1");

    /* OC22 */
    /** NAP */
    public static final Region EU_DCC_ROME_1 = new Region("eu-dcc-rome-1");

    /* OC24 */
    /** AVZ */
    public static final Region EU_DCC_ZURICH_1 = new Region("eu-dcc-zurich-1");

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
        OC1_REGIONS.put(EU_MADRID_1.getRegionId(), EU_MADRID_1);
        OC1_REGIONS.put(EU_MARSEILLE_1.getRegionId(), EU_MARSEILLE_1);
        OC1_REGIONS.put(EU_MILAN_1.getRegionId(), EU_MILAN_1);
        OC1_REGIONS.put(EU_PARIS_1.getRegionId(), EU_PARIS_1);
        OC1_REGIONS.put(EU_STOCKHOLM_1.getRegionId(), EU_STOCKHOLM_1);
        OC1_REGIONS.put(EU_ZURICH_1.getRegionId(), EU_ZURICH_1);

        OC1_REGIONS.put(ME_ABUDHABI_1.getRegionId(), ME_ABUDHABI_1);
        OC1_REGIONS.put(ME_DUBAI_1.getRegionId(), ME_DUBAI_1);
        OC1_REGIONS.put(ME_JEDDAH_1.getRegionId(), ME_JEDDAH_1);

        OC1_REGIONS.put(MX_QUERETARO_1.getRegionId(), MX_QUERETARO_1);
        OC1_REGIONS.put(MX_MONTERREY_1.getRegionId(), MX_MONTERREY_1);

        OC1_REGIONS.put(IL_JERUSALEM_1.getRegionId(), IL_JERUSALEM_1);

        /* LAD */
        OC1_REGIONS.put(SA_SAOPAULO_1.getRegionId(), SA_SAOPAULO_1);
        OC1_REGIONS.put(SA_SANTIAGO_1.getRegionId(), SA_SANTIAGO_1);
        OC1_REGIONS.put(SA_VALPARAISO_1.getRegionId(), SA_VALPARAISO_1);
        OC1_REGIONS.put(SA_VINHEDO_1.getRegionId(), SA_VINHEDO_1);

        /* North America */
        OC1_REGIONS.put(US_ASHBURN_1.getRegionId(), US_ASHBURN_1);
        OC1_REGIONS.put(US_PHOENIX_1.getRegionId(), US_PHOENIX_1);
        OC1_REGIONS.put(US_SANJOSE_1.getRegionId(), US_SANJOSE_1);
        OC1_REGIONS.put(US_SALTLAKE_2.getRegionId(), US_SALTLAKE_2);
        OC1_REGIONS.put(US_CHICAGO_1.getRegionId(), US_CHICAGO_1);

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

        /* OC5 */
        OC5_REGIONS.put(US_TACOMA_1.getRegionId(), US_TACOMA_1);

        /* OC8 */
        OC8_REGIONS.put(AP_CHIYODA_1.getRegionId(), AP_CHIYODA_1);
        OC8_REGIONS.put(AP_IBARAKI_1.getRegionId(), AP_IBARAKI_1);

        /* OC9 */
        OC9_REGIONS.put(ME_DCC_MUSCAT_1.getRegionId(), ME_DCC_MUSCAT_1);

        /* OC10 */
        OC10_REGIONS.put(AP_DCC_CANBERRA_1.getRegionId(), AP_DCC_CANBERRA_1);

        /* OC14 */
        OC14_REGIONS.put(AP_DCC_DUBLIN_1.getRegionId(), AP_DCC_DUBLIN_1);
        OC14_REGIONS.put(AP_DCC_DUBLIN_2.getRegionId(), AP_DCC_DUBLIN_2);
        OC14_REGIONS.put(AP_DCC_MILAN_1.getRegionId(), AP_DCC_MILAN_1);
        OC14_REGIONS.put(AP_DCC_MILAN_2.getRegionId(), AP_DCC_MILAN_2);
        OC14_REGIONS.put(AP_DCC_RATING_1.getRegionId(), AP_DCC_RATING_1);
        OC14_REGIONS.put(AP_DCC_RATING_2.getRegionId(), AP_DCC_RATING_2);

        /* OC16 */
        OC16_REGIONS.put(US_WESTJORDAN_1.getRegionId(), US_WESTJORDAN_1);

        /* OC17 */
        OC17_REGIONS.put(US_DCC_PHOENIX_1.getRegionId(), US_DCC_PHOENIX_1);
        OC17_REGIONS.put(US_DCC_PHOENIX_2.getRegionId(), US_DCC_PHOENIX_2);
        OC17_REGIONS.put(US_DCC_PHOENIX_4.getRegionId(), US_DCC_PHOENIX_4);

        /* OC19 */
        OC19_REGIONS.put(EU_FRANKFURT_2.getRegionId(), EU_FRANKFURT_2);
        OC19_REGIONS.put(EU_MADRID_2.getRegionId(), EU_MADRID_2);

        /* OC20 */
        OC20_REGIONS.put(EU_JOVANOVAC_1.getRegionId(), EU_JOVANOVAC_1);

        /* OC22 */
        OC22_REGIONS.put(EU_DCC_ROME_1.getRegionId(), EU_DCC_ROME_1);

        /* OC24 */
        OC24_REGIONS.put(EU_DCC_ZURICH_1.getRegionId(), EU_DCC_ZURICH_1);
    }

    private final static MessageFormat OC1_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud.com");
    private final static MessageFormat GOV_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclegovcloud.com");
    private final static MessageFormat OC4_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclegovcloud.uk");
    private final static MessageFormat OC5_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud5.com");
    private final static MessageFormat OC8_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud8.com");
    private final static MessageFormat OC9_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud9.com");
    private final static MessageFormat OC10_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud10.com");
    private final static MessageFormat OC14_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud14.com");
    private final static MessageFormat OC16_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud16.com");
    private final static MessageFormat OC17_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud17.com");
    private final static MessageFormat OC19_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud.eu");
    private final static MessageFormat OC20_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud20.com");
    private final static MessageFormat OC22_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud22.com");
    private final static MessageFormat OC24_EP_BASE = new MessageFormat(
        "https://nosql.{0}.oci.oraclecloud24.com");

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
        if (isOC5Region(regionId)) {
            return OC5_EP_BASE.format(new Object[] { regionId });
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
        if (isOC14Region(regionId)) {
            return OC14_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC16Region(regionId)) {
            return OC16_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC17Region(regionId)) {
            return OC17_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC19Region(regionId)) {
            return OC19_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC20Region(regionId)) {
            return OC20_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC22Region(regionId)) {
            return OC22_EP_BASE.format(new Object[] { regionId });
        }
        if (isOC24Region(regionId)) {
            return OC24_EP_BASE.format(new Object[] { regionId });
        }
        throw new IllegalArgumentException(
            "Unable to find endpoint for unknown region" + regionId);
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
            region = OC5_REGIONS.get(regionId);
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
        if (region == null) {
            region = OC14_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC16_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC17_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC19_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC20_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC22_REGIONS.get(regionId);
        }
        if (region == null) {
            region = OC24_REGIONS.get(regionId);
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
    public static boolean isOC5Region(String regionId) {
        return (OC5_REGIONS.get(regionId) != null);
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
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC14Region(String regionId) {
        return (OC14_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC16Region(String regionId) {
        return (OC16_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC17Region(String regionId) {
        return (OC17_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC19Region(String regionId) {
        return (OC19_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC20Region(String regionId) {
        return (OC20_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC22Region(String regionId) {
        return (OC22_REGIONS.get(regionId) != null);
    }

    /**
     * @hidden
     * Internal use only
     * @param regionId the region id
     * @return the value
     */
    public static boolean isOC24Region(String regionId) {
        return (OC24_REGIONS.get(regionId) != null);
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
    public static Collection<Region> getOC5Regions() {
        return OC5_REGIONS.values();
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
     * @return the regions
     */
    public static Collection<Region> getOC14Regions() {
        return OC14_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC16Regions() {
        return OC16_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC17Regions() {
        return OC17_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC19Regions() {
        return OC19_REGIONS.values();
    }

    /**
     * @hidden
     * Internal use only
     * @return the regions
     */
    public static Collection<Region> getOC20Regions() {
        return OC20_REGIONS.values();
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
