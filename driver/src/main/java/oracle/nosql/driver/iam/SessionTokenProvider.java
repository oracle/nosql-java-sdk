/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.OCIConfigFileReader.*;
import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.function.Supplier;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;

/**
 * @hidden
 * Internal use only
 * <p>
 * The authentication profile provider used to call service API using token
 * based authentication.
 */
class SessionTokenProvider
    implements AuthenticationProfileProvider, RegionProvider{

    private final String sessionTokenFilePath;
    private final String tenantId;
    private final Supplier<InputStream> privateKeySupplier;
    private final char[] passphrase;
    private final Region region;

    /**
     * Creates a new instance using the config file at the default location and
     * the default profile.
     */
    SessionTokenProvider() {
        this(DEFAULT_FILE_PATH, DEFAULT_PROFILE_NAME);
    }

    /**
     * Creates a new instance using the config file at the default location,
     * @param profile profile to load
     */
    SessionTokenProvider(String profile) {
        this(DEFAULT_FILE_PATH, profile);
    }

    /**
     * Creates a new instance.
     *
     * @param configFilePath path to the OCI configuration file
     * @param profile profile to load, optional
     */
    SessionTokenProvider(String configFilePath, String profile) {
        final OCIConfigFile configFile;
        try {
            configFile = OCIConfigFileReader.parse(configFilePath, profile);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Error parsing config file " + configFilePath +
                ": " + e.getMessage());
        }

        this.sessionTokenFilePath = configFile.get(SESSION_TOKEN_FILE_PROP);
        requireNonNullIAE(sessionTokenFilePath, missing(SESSION_TOKEN_FILE_PROP));
        this.tenantId = configFile.get(TENANCY_PROP);
        requireNonNullIAE(tenantId, missing(TENANCY_PROP));
        final String keyFilePath = configFile.get(KEY_FILE_PROP);
        requireNonNullIAE(tenantId, missing(KEY_FILE_PROP));
        this.privateKeySupplier = new PrivateKeyFileSupplier(
            new File(Utils.expandUserHome(keyFilePath)));
        this.region = getRegionFromConfigFile(configFile);

        /* Optional parameters */
        final String passphraseString = configFile.get(PASSPHRASE_PROP);
        this.passphrase = (passphraseString != null) ?
            passphraseString.toCharArray() : null;
    }

    private String refreshAndGetTokenInternal() {
        final File sessionTokenFile = new File(
            Utils.expandUserHome(sessionTokenFilePath));
        final StringBuilder token = new StringBuilder();

        try (Scanner reader = new Scanner(sessionTokenFile)) {
            while (reader.hasNextLine()) {
                token.append(reader.nextLine());
            }
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(
                "Session token file " + sessionTokenFilePath + " not found");
        }

        return token.toString();
    }

    public String getTenantId() {
        return this.tenantId;
    }

    @Override
    public Region getRegion() {
        return this.region;
    }

    @Override
    public String getKeyId() {
        return "ST$" + refreshAndGetTokenInternal();
    }

    @Override
    public InputStream getPrivateKey() {
        return privateKeySupplier.get();
    }

    @Override
    public char[] getPassphraseCharacters() {
        return passphrase;
    }
}
