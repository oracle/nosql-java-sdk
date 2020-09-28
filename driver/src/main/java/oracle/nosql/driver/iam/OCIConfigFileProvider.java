/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.OCIConfigFileReader.OCIConfigFile;
import oracle.nosql.driver.util.CheckNull;

/**
 * @hidden
 * Internal use only
 * <p>
 * User authentication profile provider using Oracle Cloud Infrastructure
 * common configuration file. The configuration property name and value must
 * be written in the form of <pre>key=value</pre> This provider parses each
 * line of the specified file, extracting properties. Leading and trailing
 * whitespace in key and value are ignored.
 * <p>
 * These property names are recognized and are case-sensitive:
 *
 * <ul>
 * <li>fingerprint</li>
 * <li>tenancy</li>
 * <li>user</li>
 * <li>key_file</li>
 * <li>pass_phrase</li>
 * </ul>
 */
class OCIConfigFileProvider
    implements UserAuthenticationProfileProvider, RegionProvider {

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

    private final SimpleProfileProvider delegate;

    /**
     * Creates a builder using the config file at the default location
     * DEFAULT_FILE_PATH, will provide DEFAULT profile.
     *
     * @throws IOException if the configuration file could not be loaded
     */
    public OCIConfigFileProvider()
        throws IOException {

        this(OCIConfigFileReader.parse(DEFAULT_FILE_PATH,
                                       DEFAULT_PROFILE_NAME));
    }

    /**
     * Creates a provider using the config file at the default location
     * DEFAULT_FILE_PATH, will provide specified profile.
     *
     * @param profile profile to load. If null, use the DEFAULT.
     * @throws IOException if the configuration file could not be loaded
     */
    public OCIConfigFileProvider(String profile)
        throws IOException {

        this(OCIConfigFileReader.parse(DEFAULT_FILE_PATH, profile));
    }

    /**
     * Creates a provider with given config file path, will provide specified
     * profile.
     *
     * @param configurationFilePath path to the OCI configuration file
     * @param profile profile to load. If null, use the DEFAULT.
     * @throws IOException if the configuration file could not be loaded
     */
    public OCIConfigFileProvider(String configurationFilePath, String profile)
        throws IOException {

        this(OCIConfigFileReader.parse(configurationFilePath, profile));
    }

    private OCIConfigFileProvider(OCIConfigFile configFile) {
        String fingerprint = configFile.get(FINGERPRINT_PROP);
        CheckNull.requireNonNull(fingerprint, missing(FINGERPRINT_PROP));
        String tenantId = configFile.get(TENANCY_PROP);
        CheckNull.requireNonNull(tenantId, missing(TENANCY_PROP));
        String userId = configFile.get(USER_PROP);
        CheckNull.requireNonNull(userId, missing(USER_PROP));
        String pemFilePath = configFile.get(KEY_FILE_PROP);
        CheckNull.requireNonNull(pemFilePath, missing(KEY_FILE_PROP));

        /* passphrase is optional */
        String passPhrase = configFile.get(PASSPHRASE_PROP);

        Supplier<InputStream> privateKeySupplier =
            new PrivateKeyFileSupplier(
                new File(Utils.expandUserHome(pemFilePath)));

        SimpleProfileProvider.SimpleProfileProviderBuilder builder =
             SimpleProfileProvider.builder()
             .fingerprint(fingerprint)
             .privateKeySupplier(privateKeySupplier)
             .tenantId(tenantId)
             .userId(userId);

        if (passPhrase != null) {
            builder = builder.passphrase(passPhrase.toCharArray());
        }

        /* region is optional */
        String regionValue = configFile.get(REGION_PROP);
        if (regionValue != null) {
            builder.region(Region.fromRegionId(regionValue));
        }
        this.delegate = builder.build();

    }

    @Override
    public String getFingerprint() {
        return this.delegate.getFingerprint();
    }

    @Override
    public String getTenantId() {
        return this.delegate.getTenantId();
    }

    @Override
    public String getUserId() {
        return this.delegate.getUserId();
    }

    @Override
    public char[] getPassphraseCharacters() {
        return this.delegate.getPassphraseCharacters();
    }

    @Override
    public InputStream getPrivateKey() {
        return this.delegate.getPrivateKey();
    }

    @Override
    public String getKeyId() {
        return this.delegate.getKeyId();
    }

    @Override
    public Region getRegion() {
        return this.delegate.getRegion();
    }

    private String missing(String propertyName) {
        return "missing " + propertyName + " in config";
    }
}
