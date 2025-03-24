/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/* used for parsing region config file which is JSON */
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

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
    /* Region metadata env attribute key */
    private static final String OCI_REGION_METADATA_ENV_VAR_NAME =
        "OCI_REGION_METADATA";

    /* Default realm metadata env attribute key - not used by nosql at this time
    private static final String OCI_DEFAULT_REALM_ENV_VAR_NAME =
        "OCI_DEFAULT_REALM";
    */

    /* The regions-config file path location */
    private static final String REGIONS_CONFIG_FILE_PATH =
        System.getProperty("user.home") + File.separator +
            ".oci" + File.separator + "regions-config.json";

    /*
     * endpoint format: {service}.{regionId}.oci.{realmDomain}, e.g.
     *   https://nosql.us-ashburn-1.oci.oraclecloud.com
     */
    private static final String svcEndpointFormat =
        "https://%1$s.%2$s.oci.%3$s";
    private static final String authEndpointFormat =
        "https://%1$s.%2$s.%3$s";

    /* LinkedHashMap to ensure stable ordering of registered regions */
    private static final Map<String, Region> ALL_REGIONS =
        new LinkedHashMap<>();

    /* only do each of these once, and use of IMDS must be enabled */
    private static volatile boolean hasUsedEnvVar = false;
    private static volatile boolean hasUsedConfigFile = false;
    private static volatile boolean visitIMDS = false;

    /* synchronization of static structures */
    private static final ReentrantReadWriteLock lock =
        new ReentrantReadWriteLock();
    private static final Lock readLock = lock.readLock();
    private static final Lock writeLock = lock.writeLock();

    /*
     * Do not edit from here down to the end of the list of generated variables
     */
    /* Known Regions start -- automatically generated */
    public static final Region AF_JOHANNESBURG_1 = register("af-johannesburg-1", Realm.OC1, "jnb");
    public static final Region AP_CHUNCHEON_1 = register("ap-chuncheon-1", Realm.OC1, "yny");
    public static final Region AP_HYDERABAD_1 = register("ap-hyderabad-1", Realm.OC1, "hyd");
    public static final Region AP_MELBOURNE_1 = register("ap-melbourne-1", Realm.OC1, "mel");
    public static final Region AP_MUMBAI_1 = register("ap-mumbai-1", Realm.OC1, "bom");
    public static final Region AP_OSAKA_1 = register("ap-osaka-1", Realm.OC1, "kix");
    public static final Region AP_SEOUL_1 = register("ap-seoul-1", Realm.OC1, "icn");
    public static final Region AP_SINGAPORE_1 = register("ap-singapore-1", Realm.OC1, "sin");
    public static final Region AP_SINGAPORE_2 = register("ap-singapore-2", Realm.OC1, "xsp");
    public static final Region AP_SYDNEY_1 = register("ap-sydney-1", Realm.OC1, "syd");
    public static final Region AP_TOKYO_1 = register("ap-tokyo-1", Realm.OC1, "nrt");
    public static final Region CA_MONTREAL_1 = register("ca-montreal-1", Realm.OC1, "yul");
    public static final Region CA_TORONTO_1 = register("ca-toronto-1", Realm.OC1, "yyz");
    public static final Region EU_AMSTERDAM_1 = register("eu-amsterdam-1", Realm.OC1, "ams");
    public static final Region EU_FRANKFURT_1 = register("eu-frankfurt-1", Realm.OC1, "fra");
    public static final Region EU_MADRID_1 = register("eu-madrid-1", Realm.OC1, "mad");
    public static final Region EU_MARSEILLE_1 = register("eu-marseille-1", Realm.OC1, "mrs");
    public static final Region EU_MILAN_1 = register("eu-milan-1", Realm.OC1, "lin");
    public static final Region EU_PARIS_1 = register("eu-paris-1", Realm.OC1, "cdg");
    public static final Region EU_STOCKHOLM_1 = register("eu-stockholm-1", Realm.OC1, "arn");
    public static final Region EU_ZURICH_1 = register("eu-zurich-1", Realm.OC1, "zrh");
    public static final Region IL_JERUSALEM_1 = register("il-jerusalem-1", Realm.OC1, "mtz");
    public static final Region ME_ABUDHABI_1 = register("me-abudhabi-1", Realm.OC1, "auh");
    public static final Region ME_DUBAI_1 = register("me-dubai-1", Realm.OC1, "dxb");
    public static final Region ME_JEDDAH_1 = register("me-jeddah-1", Realm.OC1, "jed");
    public static final Region ME_RIYADH_1 = register("me-riyadh-1", Realm.OC1, "ruh");
    public static final Region MX_MONTERREY_1 = register("mx-monterrey-1", Realm.OC1, "mty");
    public static final Region MX_QUERETARO_1 = register("mx-queretaro-1", Realm.OC1, "qro");
    public static final Region SA_BOGOTA_1 = register("sa-bogota-1", Realm.OC1, "bog");
    public static final Region SA_SANTIAGO_1 = register("sa-santiago-1", Realm.OC1, "scl");
    public static final Region SA_SAOPAULO_1 = register("sa-saopaulo-1", Realm.OC1, "gru");
    public static final Region SA_VALPARAISO_1 = register("sa-valparaiso-1", Realm.OC1, "vap");
    public static final Region SA_VINHEDO_1 = register("sa-vinhedo-1", Realm.OC1, "vcp");
    public static final Region UK_LONDON_1 = register("uk-london-1", Realm.OC1, "lhr");
    public static final Region UK_CARDIFF_1 = register("uk-cardiff-1", Realm.OC1, "cwl");
    public static final Region US_PHOENIX_1 = register("us-phoenix-1", Realm.OC1, "phx");
    public static final Region US_ASHBURN_1 = register("us-ashburn-1", Realm.OC1, "iad");
    public static final Region US_SALTLAKE_2 = register("us-saltlake-2", Realm.OC1, "aga");
    public static final Region US_SANJOSE_1 = register("us-sanjose-1", Realm.OC1, "sjc");
    public static final Region US_CHICAGO_1 = register("us-chicago-1", Realm.OC1, "ord");
    public static final Region US_LANGLEY_1 = register("us-langley-1", Realm.OC2, "lfi");
    public static final Region US_LUKE_1 = register("us-luke-1", Realm.OC2, "luf");
    public static final Region US_GOV_ASHBURN_1 = register("us-gov-ashburn-1", Realm.OC3, "ric");
    public static final Region US_GOV_CHICAGO_1 = register("us-gov-chicago-1", Realm.OC3, "pia");
    public static final Region US_GOV_PHOENIX_1 = register("us-gov-phoenix-1", Realm.OC3, "tus");
    public static final Region UK_GOV_LONDON_1 = register("uk-gov-london-1", Realm.OC4, "ltn");
    public static final Region UK_GOV_CARDIFF_1 = register("uk-gov-cardiff-1", Realm.OC4, "brs");
    public static final Region US_TACOMA_1 = register("us-tacoma-1", Realm.OC5, "tiw");
    public static final Region AP_CHIYODA_1 = register("ap-chiyoda-1", Realm.OC8, "nja");
    public static final Region AP_IBARAKI_1 = register("ap-ibaraki-1", Realm.OC8, "ukb");
    public static final Region ME_DCC_MUSCAT_1 = register("me-dcc-muscat-1", Realm.OC9, "mct");
    public static final Region AP_DCC_CANBERRA_1 = register("ap-dcc-canberra-1", Realm.OC10, "wga");
    public static final Region EU_DCC_DUBLIN_1 = register("eu-dcc-dublin-1", Realm.OC14, "ork");
    public static final Region EU_DCC_DUBLIN_2 = register("eu-dcc-dublin-2", Realm.OC14, "snn");
    public static final Region EU_DCC_MILAN_1 = register("eu-dcc-milan-1", Realm.OC14, "bgy");
    public static final Region EU_DCC_MILAN_2 = register("eu-dcc-milan-2", Realm.OC14, "mxp");
    public static final Region EU_DCC_RATING_1 = register("eu-dcc-rating-1", Realm.OC14, "dus");
    public static final Region EU_DCC_RATING_2 = register("eu-dcc-rating-2", Realm.OC14, "dtm");
    public static final Region AP_DCC_GAZIPUR_1 = register("ap-dcc-gazipur-1", Realm.OC15, "dac");
    public static final Region US_WESTJORDAN_1 = register("us-westjordan-1", Realm.OC16, "sgu");
    public static final Region US_DCC_PHOENIX_1 = register("us-dcc-phoenix-1", Realm.OC17, "ifp");
    public static final Region US_DCC_PHOENIX_2 = register("us-dcc-phoenix-2", Realm.OC17, "gcn");
    public static final Region US_DCC_PHOENIX_4 = register("us-dcc-phoenix-4", Realm.OC17, "yum");
    public static final Region EU_FRANKFURT_2 = register("eu-frankfurt-2", Realm.OC19, "str");
    public static final Region EU_MADRID_2 = register("eu-madrid-2", Realm.OC19, "vll");
    public static final Region EU_JOVANOVAC_1 = register("eu-jovanovac-1", Realm.OC20, "beg");
    public static final Region ME_DCC_DOHA_1 = register("me-dcc-doha-1", Realm.OC21, "doh");
    public static final Region EU_DCC_ROME_1 = register("eu-dcc-rome-1", Realm.OC22, "nap");
    public static final Region US_SOMERSET_1 = register("us-somerset-1", Realm.OC23, "ebb");
    public static final Region US_THAMES_1 = register("us-thames-1", Realm.OC23, "ebl");
    public static final Region EU_DCC_ZURICH_1 = register("eu-dcc-zurich-1", Realm.OC24, "avz");
    public static final Region EU_CRISSIER_1 = register("eu-crissier-1", Realm.OC24, "avf");
    public static final Region AP_DCC_OSAKA_1 = register("ap-dcc-osaka-1", Realm.OC25, "uky");
    public static final Region AP_DCC_TOKYO_1 = register("ap-dcc-tokyo-1", Realm.OC25, "tyo");
    public static final Region ME_ABUDHABI_3 = register("me-abudhabi-3", Realm.OC26, "ahu");
    public static final Region ME_ALAIN_1 = register("me-alain-1", Realm.OC26, "rba");
    public static final Region US_DCC_SWJORDAN_1 = register("us-dcc-swjordan-1", Realm.OC27, "ozz");
    public static final Region US_DCC_SWJORDAN_2 = register("us-dcc-swjordan-2", Realm.OC28, "drs");
    public static final Region ME_ABUDHABI_2 = register("me-abudhabi-2", Realm.OC29, "rkt");
    public static final Region ME_ABUDHABI_4 = register("me-abudhabi-4", Realm.OC29, "shj");
    public static final Region AP_HOBSONVILLE_1 = register("ap-hobsonville-1", Realm.OC31, "izq");
    public static final Region AP_SILVERDALE_1 = register("ap-silverdale-1", Realm.OC31, "jjt");
    public static final Region AP_SUWON_1 = register("ap-suwon-1", Realm.OC35, "dln");
    public static final Region AP_SEOUL_2 = register("ap-seoul-2", Realm.OC35, "dtz");
    public static final Region AP_CHUNCHEON_2 = register("ap-chuncheon-2", Realm.OC35, "bno");
    /* Known Regions end generated code */

    /* instance state */
    private final String regionId;
    private final String regionCode;
    private final Realm realm;

    private Region(String regionId, String regionCode, Realm realm) {
        this.regionId = regionId;
        this.regionCode = regionCode;
        this.realm = realm;
        writeLock.lock();
        try {
            ALL_REGIONS.put(regionId, this);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the Region object from the canonical public region id. Returns
     * null if the region id is not known.
     *
     * @param regionId The region ID.
     * @return The Region object.
     */
    public static Region fromRegionId(String regionId) {
        if (regionId == null) {
            throw new IllegalArgumentException("Invalid region id " + regionId);
        }
        regionId = regionId.toLowerCase();
        Region region = ALL_REGIONS.get(regionId);
        if (region == null) {
            registerAllRegions();
            region = ALL_REGIONS.get(regionId);
        }
        return region;
    }

    /**
     * Returns the Region object from the public region code or id. Returns
     * null if the region id and code are not known
     *
     * @param regionIdOrCode The region code or id.
     * @return The Region object.
     */
    public static Region fromRegionIdOrCode(String regionIdOrCode) {
        final String rCodeOrId = regionIdOrCode.toLowerCase();
        Region region = fromRegionId(rCodeOrId);
        if (region != null) {
            return region;
        }
        /*
         * Searching based on code requires a search of all values
         */
        readLock.lock();
        try {
            Optional<Region> reg = ALL_REGIONS.values().stream()
                .filter(
                    r ->
                    r.regionCode.equals(rCodeOrId)
                    || r.regionId.equals(rCodeOrId))
                .findAny();
            if (reg.isPresent()) {
                return reg.get();
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the region id for this region
     * @return the region id
     */
    public String getRegionId() {
        return regionId;
    }

    /**
     * Returns the NoSQL Database Cloud Service Endpoint for this region.
     * @return NoSQL Database Cloud Service Endpoint
     */
    public String endpoint() {
        /*
         * endpoint format: nosql.{regionID}.oci.{realmDomain}
         */
        return endpointForService("nosql", svcEndpointFormat);
    }

    /**
     * Returns the Endpoint for this region for the named service.
     * @return Service Endpoint
     * @hidden
     */
    public String endpointForService(String service, String format) {
        /*
         * endpoint format: {service}.{regionID}[.oci].{realmDomain}
         */
        return String.format(format, service, regionId,
                             realm.getSecondLevelDomain());
    }

    /**
     * Returns the format for the auth endpoint
     * @return the format
     * @hidden
     */
    public static String getAuthEndpointFormat() {
        return authEndpointFormat;
    }

    /**
     * Register a new region. Used to allow the SDK to be forward compatible
     * with unreleased regions.
     *
     * @param regionId The region ID.
     * @param realm The realm of the new region.
     * @param regionCode The 3-letter region code returned by the instance
     * metadata service as the 'region' value, if it differs from regionId.
     * This is only needed for very early regions.
     * @return The registered region (or existing one if found).
     */
    static Region register(String regionId,
                           final Realm realm,
                           String regionCode) {
        regionId = regionId.trim().toLowerCase(Locale.US);
        if (regionId.isEmpty()) {
            throw new IllegalArgumentException("Cannot have empty regionId");
        }

        Region region;
        readLock.lock();
        try {
            region = getRegion(regionId, realm);
            if (region != null) {
                return region;
            }
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            /* Recheck in case of race */
            region = getRegion(regionId, realm);
            if (region != null) {
                return region;
            }
            if (regionCode != null) {
                regionCode = regionCode.trim().toLowerCase(Locale.US);
                if (regionCode.isEmpty()) {
                    regionCode = null;
                }
            }
            return new Region(regionId, regionCode, realm);
        } finally {
            writeLock.unlock();
        }
    }

    private static Region getRegion(String regionId, Realm realm) {
        readLock.lock();
        try {
            for (Region region : ALL_REGIONS.values()) {
                if (region.regionId.equals(regionId)) {
                    if (!region.realm.equals(realm)) {
                        throw new IllegalArgumentException(
                            "Region : "
                            + regionId
                            + " is already registered with "
                            + region.realm
                            + ". It cannot be re-registered with a different"
                            + " realm.");
                    }
                    return region;
                }
            }
        } finally {
            readLock.unlock();
        }
        return null;
    }

    static Region[] values() {
        registerAllRegions();
        readLock.lock();
        try {
            return ALL_REGIONS.values().toArray(new Region[ALL_REGIONS.size()]);
        } finally {
            readLock.unlock();
        }
    }

    /** Register all regions */
    private static void registerAllRegions() {
        if (!hasUsedConfigFile) {
            readRegionConfigFile();
        }

        if (!hasUsedEnvVar) {
            readEnvVar();
        }
    }

    /*
     * public for testing
     */
    public static void readEnvVar() {
        final String envVar =
            System.getProperty(OCI_REGION_METADATA_ENV_VAR_NAME);

        hasUsedEnvVar = true;
        if (envVar != null) {
            try {
                MapValue region =
                    FieldValue.createFromJson(envVar, null).asMap();
                addRegion(region);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Unable to add region from environment variable " +
                    envVar + ": " + e.getMessage());
            }
        }
    }

    /* Registers region and sets hasUsedConfigFile status to true. */
    private static void readRegionConfigFile() {
        hasUsedConfigFile = true;
        readRegionConfigFile(REGIONS_CONFIG_FILE_PATH);
    }

    /*
     * this is public for testing purposes only
     */
    public static void readRegionConfigFile(String fileName) {
        File file = new File(fileName);
        if (!file.isFile()) {
            /* not an error, file doesn't exist or isn't regular file */
            return;
        }
        String content = null;
        try {
            content = new String(
                Files.readAllBytes(Paths.get(fileName)),
                StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Unable to read regions file, " + fileName + ": " + ioe);
        }
        if (content == null || content.isEmpty()) {
            /* empty file is not an error */
            return;
        }
        ArrayValue arrayOfRegions =
            FieldValue.createFromJson(content, null).asArray();
        for (FieldValue val : arrayOfRegions) {
            addRegion(val.asMap());
        }
    }

    private static void addRegion(MapValue region) {
        /* this will throw on invalid region */
        validateRegion(region);

        /* this will add the realm if not present */
        Realm realm =
            Realm.getRealm(region.get("realmKey").getString(),
                           region.get("realmDomainComponent").getString(),
                           true);
        register(region.get("regionIdentifier").getString(),
                 realm,
                 region.get("regionKey").getString());
    }

    /*
     * Make sure that all required fields are present
     */
    private static void validateRegion(MapValue region) {
        final String[] fields = new String[] {"regionKey",
                                              "realmDomainComponent",
                                              "realmKey",
                                              "regionIdentifier"};
        for (String f : fields) {
            try {
                if (!region.get(f).isString()) {
                    throw new IllegalArgumentException(
                        "Type of field " + f + " in region must be string: " +
                        region);
                }
            } catch (NullPointerException npe) {
                throw new IllegalArgumentException(
                    "Missing field " + f + " in region: " + region);
            }
        }
    }

    /*
     * Realm is internal use for now
     */
    static class Realm {
        /* LinkedHashMap to ensure stable ordering of registered realms */
        private static final Map<String, Realm> ALL_REALMS =
            new LinkedHashMap<>();
        private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private static final Lock readLock = lock.readLock();
        private static final Lock writeLock = lock.writeLock();

        private final String realmId;
        private final String secondLevelDomain;

        /*
         * Do not manually modify from here to 'Known Realms end' comment
         * This is generated code
         */

        /* Known Realms start -- automatically generated */
        static final Realm OC1 = new Realm("oraclecloud.com", "oc1");
        static final Realm OC2 = new Realm("oraclegovcloud.com", "oc2");
        static final Realm OC3 = new Realm("oraclegovcloud.com", "oc3");
        static final Realm OC4 = new Realm("oraclegovcloud.uk", "oc4");
        static final Realm OC5 = new Realm("oraclecloud5.com", "oc5");
        static final Realm OC8 = new Realm("oraclecloud8.com", "oc8");
        static final Realm OC9 = new Realm("oraclecloud9.com", "oc9");
        static final Realm OC10 = new Realm("oraclecloud10.com", "oc10");
        static final Realm OC14 = new Realm("oraclecloud14.com", "oc14");
        static final Realm OC15 = new Realm("oraclecloud15.com", "oc15");
        static final Realm OC16 = new Realm("oraclecloud16.com", "oc16");
        static final Realm OC17 = new Realm("oraclecloud17.com", "oc17");
        static final Realm OC19 = new Realm("oraclecloud.eu", "oc19");
        static final Realm OC20 = new Realm("oraclecloud20.com", "oc20");
        static final Realm OC21 = new Realm("oraclecloud21.com", "oc21");
        static final Realm OC22 = new Realm("psn-pco.it", "oc22");
        static final Realm OC23 = new Realm("oraclecloud23.com", "oc23");
        static final Realm OC24 = new Realm("oraclecloud24.com", "oc24");
        static final Realm OC25 = new Realm("nricloud.jp", "oc25");
        static final Realm OC26 = new Realm("oraclecloud26.com", "oc26");
        static final Realm OC27 = new Realm("oraclecloud27.com", "oc27");
        static final Realm OC28 = new Realm("oraclecloud28.com", "oc28");
        static final Realm OC29 = new Realm("oraclecloud29.com", "oc29");
        static final Realm OC31 = new Realm("sovereigncloud.nz", "oc31");
        static final Realm OC35 = new Realm("oraclecloud35.com", "oc35");
        /* Known Realms end generated code */

        private Realm(String secondLevelDomain, String realmId) {
            this.realmId = realmId;
            this.secondLevelDomain = secondLevelDomain;
            writeLock.lock();
            try {
                ALL_REALMS.put(realmId, this);
            } finally {
                writeLock.unlock();
            }
        }

        static Realm getRealm(String realmId, String secondLevelDomain,
                              boolean add) {
            Realm realm = ALL_REALMS.get(realmId);
            if (realm == null && add) {
                realm = new Realm(realmId, secondLevelDomain);
            }
            return realm;
        }

        String getRealmId() {
            return this.realmId;
        }

        String getSecondLevelDomain() {
            return this.secondLevelDomain;
        }

        /**
         * All known Realms in this version of the SDK
         *
         * @return Known realms
         */
        static Realm[] values() {
            readLock.lock();
            try {
                return ALL_REALMS.values().toArray(
                    new Realm[ALL_REALMS.size()]);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * Register a new Realm. Used to allow the SDK to be forward
         * compatible with unreleased realms.
         *
         * @param realmId The realm id.
         * @param secondLevelDomain The second level domain of the realm.
         * @return The registered Realm (or existing one if found).
         */
        static Realm register(String realmId, String secondLevelDomain) {
            realmId = realmId.toLowerCase(Locale.US);
            secondLevelDomain = secondLevelDomain.toLowerCase(Locale.US);
            writeLock.lock();
            try {
                for (Realm realm : Realm.values()) {
                    if (realm.realmId.equals(realmId)) {
                        if (!realm.secondLevelDomain.equals(secondLevelDomain)){
                            throw new IllegalArgumentException(
                                "RealmId : "
                                + realmId
                                + " is already registered with "
                                + realm.getSecondLevelDomain()
                                + ". It cannot be re-registered with a " +
                                "different secondLevelDomain");
                        }
                        return realm;
                    }
                }
                return new Realm(realmId, secondLevelDomain);
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) return true;
            if (!(o instanceof Realm)) return false;
            final Realm other = (Realm) o;
            return realmId.equals(other.realmId) &&
                secondLevelDomain.equals(other.secondLevelDomain);
        }

        @Override
        public int hashCode() {
            return realmId.hashCode() + secondLevelDomain.hashCode();
        }
    }

    /**
     * Internal use only
     * @hidden
     */
    public interface RegionProvider {

        /**
         * Returns region
         * @return the Region to use for NoSQLHandle
         * @hidden
         */
        Region getRegion();
    }
}
