/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.Region;

/**
 * Cloud service only.
 * <p>
 * An instance that reads an Oracle Cloud Infrastructure (OCI) configuration file.
 * Note, an OCI configuration file <b>MUST</b> contain a "DEFAULT" profile,
 * additional profiles are optional.
 * <p>
 * An OCI configuration file allows use of custom key-value pairs, which can be
 * loaded and referenced using this class. For example, the frequently used
 * compartment OCID can be put in a configuration file.
 * <pre>
 * In the configuration file, add one custom parameter "custom_compartment_id"
 * [DEFAULT]
 * ...
 * custom_compartment_id=ocid1.compartment.oc1..aaa...
 *
 * Then it can be retrieved using this class:
 * OCIConfigFile config = OCIConfigFileReader.parse("~/.oci/config");
 * String compartmentId = config.get("custom_compartment_id");
 * </pre>
 */
public class OCIConfigFileReader {
    /**
     * Default configuration file at <code>~/.oci/config</code>
     */
    public static final String DEFAULT_FILE_PATH =
        System.getProperty("user.home") + File.separator +
            ".oci" + File.separator + "config";
    public static final String DEFAULT_PROFILE_NAME = "DEFAULT";

    static final String FINGERPRINT_PROP = "fingerprint";
    static final String TENANCY_PROP = "tenancy";
    static final String USER_PROP = "user";
    static final String KEY_FILE_PROP = "key_file";
    static final String PASSPHRASE_PROP = "pass_phrase";
    static final String REGION_PROP = "region";
    static final String SESSION_TOKEN_FILE_PROP = "security_token_file";
    static final String OCI_REGION_ENV_VAR_NAME = "OCI_REGION";

    /**
     * Create a new instance using a file at a given location.
     * <p>
     * This method is the same as calling {@link #parse(String, String)} with
     * "DEFAULT" as the profile.
     *
     * @param configFilePath The path to the config file.
     * @return A new OCIConfigFile instance.
     * @throws IOException if the file could not be read.
     */
    public static OCIConfigFile parse(String configFilePath)
        throws IOException {

        return parse(configFilePath, null);
    }

    /**
     * Create a new instance using a file at a given location.
     *
     * @param configFilePath The path to the config file.
     * @param profile The profile name to load, or null if you want to load the
     * "DEFAULT" profile.
     * @return A new OCIConfigFile instance.
     * @throws IOException if the file could not be read.
     */
    public static OCIConfigFile parse(String configFilePath, String profile)
        throws IOException {

        return parse(new FileInputStream(
            new File(Utils.expandUserHome(configFilePath))), profile);
    }

    /**
     * Create a new instance using an UTF-8 input stream.
     *
     * @param configStream The path to the config file.
     * @param profile The profile name to load, or null if you want to load the
     * "DEFAULT" profile.
     * @return A new OCIConfigFile instance.
     * @throws IOException if the file could not be read.
     */
    public static OCIConfigFile parse(InputStream configStream, String profile)
        throws IOException {

        return parse(configStream, profile, StandardCharsets.UTF_8);
    }

    /**
     * Create a new instance using an input stream.
     *
     * @param configStream The path to the config file.
     * @param profile The profile name to load, or null if you want to load the
     * "DEFAULT" profile.
     * @param charset The charset used when parsing the input stream
     * @return A new OCIConfigFile instance.
     * @throws IOException if the file could not be read.
     */
    public static OCIConfigFile parse(InputStream configStream,
                                      String profile,
                                      Charset charset)
        throws IOException {

        OCIConfigAccumulator accumulator = new OCIConfigAccumulator();
        try (BufferedReader reader = new BufferedReader(
                 new InputStreamReader(configStream, charset))) {

            String line = null;
            while ((line = reader.readLine()) != null) {
                accumulator.accept(line);
            }
        }
        if (profile != null &&
            !accumulator.configs.containsKey(profile)) {

            throw new IllegalArgumentException(
                "No profile named " + profile +
                " exists in the OCI configuration file");
        }

        return new OCIConfigFile(accumulator, profile);
    }

    static String missing(String propertyName) {
        return "Required property " + propertyName +
            " is missing from OCI configuration file." +
            " For more information about OCI configuration file and" +
            " how to get required information," +
            " see https://docs.oracle.com" +
            "/en-us/iaas/Content/API/Concepts/sdkconfig.htm";
    }

    static Region getRegionFromConfigFile(OCIConfigFile configFile) {
        Region region = null;
        String regionId = configFile.get(REGION_PROP);
        if (regionId == null || regionId.isEmpty()) {
            /* Attempts to read region id from env variable */
            regionId = System.getenv(OCI_REGION_ENV_VAR_NAME);
        }

        if (regionId != null && !regionId.isEmpty()) {
            return Region.fromRegionId(regionId);
        }
        return region;
    }

    private OCIConfigFileReader() {}

    /**
     * Cloud service only.
     * <p>
     * OCIConfigFile represents a simple lookup mechanism for an
     * OCI config file.
     */
    public static final class OCIConfigFile {
        private final OCIConfigAccumulator accumulator;
        private final String profile;

        private OCIConfigFile(OCIConfigAccumulator accumulator,
                              String profile) {
            this.accumulator = accumulator;
            this.profile = profile;
        }

        /**
         * Gets the value associated with a given key. The value returned will
         * be the one for the selected profile (if available), else the value in
         * the DEFAULT profile (if specified), else null.
         *
         * @param key the key
         * @return the value
         */
        public String get(String key) {
            if (profile != null &&
                (accumulator.configs.get(profile).containsKey(key))) {
                return accumulator.configs.get(profile).get(key);
            }
            return accumulator.foundDefaultProfile ?
                accumulator.configs.get(DEFAULT_PROFILE_NAME).get(key) :
                null;
        }
    }

    private static final class OCIConfigAccumulator {
        final Map<String, Map<String, String>> configs = new HashMap<>();
        private String currentProfile = null;
        private boolean foundDefaultProfile = false;

        private void accept(String line) {
            final String msg =
                "Invalid line in OCI configuration file, expected " +
                "\"key=value\" or \"[profile-name]\", found ";
            final String trimmedLine = line.trim();

            /* no blank lines */
            if (trimmedLine.isEmpty()) {
                return;
            }

            /* skip comments */
            if (trimmedLine.charAt(0) == '#') {
                return;
            }

            if (trimmedLine.charAt(0) == '[' &&
                trimmedLine.charAt(trimmedLine.length() - 1) == ']') {
                currentProfile = trimmedLine
                    .substring(1, trimmedLine.length() - 1).trim();

                if (currentProfile.isEmpty()) {
                    throw new IllegalArgumentException(msg + "[]");
                }
                if (currentProfile.equals(DEFAULT_PROFILE_NAME)) {
                    foundDefaultProfile = true;
                }
                if (!configs.containsKey(currentProfile)) {
                    configs.put(currentProfile, new HashMap<String, String>());
                }

                return;
            }

            final int splitIndex = trimmedLine.indexOf('=');
            if (splitIndex == -1) {
                throw new IllegalArgumentException(msg + line);
            }

            final String key = trimmedLine.substring(0, splitIndex).trim();
            final String value = trimmedLine.substring(splitIndex + 1).trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException(msg + line);
            }

            if (currentProfile == null) {
                throw new IllegalArgumentException(
                    "Invalid OCI configuration file: no profile specified");
            }

            configs.get(currentProfile).put(key, value);
        }
    }
}
