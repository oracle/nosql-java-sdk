/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;

import java.io.InputStream;
import java.util.function.Supplier;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;

/**
 * @hidden
 * Internal use only
 * <p>
 * An implementation of UserAuthenticationProfileProvider that can
 * be simply built programmatically with profiles in strings.
 */
class SimpleProfileProvider
    implements UserAuthenticationProfileProvider, RegionProvider {

    private final String tenantId;
    private final String userId;
    private final String fingerprint;
    private final char[] passphrase;
    private final Region region;

    /**
     * Supplier of the input stream with the private key. Note that this
     * stream may be read multiple times.
     */
    private final Supplier<InputStream> privateKeySupplier;


    public static SimpleProfileProviderBuilder builder() {
        return new SimpleProfileProviderBuilder();
    }

    private SimpleProfileProvider(String tenantId,
                                  String userId,
                                  String fingerprint,
                                  char[] passphrase,
                                  Supplier<InputStream> privateKeySupplier,
                                  Region region) {
        this.tenantId = tenantId;
        this.userId = userId;
        this.fingerprint = fingerprint;
        this.passphrase = passphrase;
        this.privateKeySupplier = privateKeySupplier;
        this.region = region;
    }

    @Override
    public String getKeyId() {
        requireNonNullIAE(tenantId,
                          "SimpleProfileProvider is missing tenantId");
        requireNonNullIAE(userId,
                          "SimpleProfileProvider is missing userId");
        if (!Utils.isValidOcid(getTenantId())) {
            throw new IllegalArgumentException(
                 "Tenant Id" + getTenantId() + "does not match OCID pattern");
        }
        if (!Utils.isValidOcid(getUserId())) {
            throw new IllegalArgumentException(
                 "User Id" + getUserId() + "does not match OCID pattern");
        }

        String keyId = Utils.createKeyId(this);
        return keyId;
    }

    @Override
    public InputStream getPrivateKey() {
        return privateKeySupplier.get();
    }

    @Override
    public char[] getPassphraseCharacters() {
        return passphrase;
    }

    @Override
    public String getFingerprint() {
        return fingerprint;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return String.format(
                "SimpleProfileProvider(" +
                "tenantId=%s, userId=%s, fingerprint=%s," +
                "passphraseCharacters=%s, privateKeySupplier=%s)",
                tenantId,
                userId,
                fingerprint,
                (passphrase != null ? "<provided>" :  passphrase),
                privateKeySupplier);
    }

    /**
     * Cloud service only.
     * <p>
     * Builder of SimpleProfileProvider
     */
    public static class SimpleProfileProviderBuilder {
        private String tenantId;
        private String userId;
        private String fingerprint;
        private char[] passphrase;
        private Supplier<InputStream> privateKeySupplier;
        private Region region;

        public SimpleProfileProviderBuilder tenantId(String tid) {
            this.tenantId = tid;
            return this;
        }

        public SimpleProfileProviderBuilder userId(String uid) {
            this.userId = uid;
            return this;
        }

        public SimpleProfileProviderBuilder fingerprint(String fp) {
            this.fingerprint = fp;
            return this;
        }

        public SimpleProfileProviderBuilder passphrase(char[] ph) {
            this.passphrase = ph;
            return this;
        }

        public SimpleProfileProviderBuilder region(Region r) {
            this.region = r;
            return this;
        }

        public SimpleProfileProviderBuilder
            privateKeySupplier(Supplier<InputStream> pks) {

            this.privateKeySupplier = pks;
            return this;
        }

        public SimpleProfileProvider build() {
            return new SimpleProfileProvider(
                tenantId, userId, fingerprint, passphrase,
                privateKeySupplier, region);
        }
    }
}
