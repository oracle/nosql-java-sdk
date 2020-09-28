/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.iam.ResourcePrincipalTokenSupplier.FileSecurityTokenSupplier;
import oracle.nosql.driver.iam.ResourcePrincipalTokenSupplier.FixedSecurityTokenSupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.DefaultSessionKeySupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FileKeyPairSupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FixedKeyPairSupplier;

/**
 * @hidden
 * Internal use only
 * <p>
 * The authentication profile provider used to call service API from other OCI
 * resource such as function. It authenticates with resource principal and uses
 * security token issued by IAM to do the actual request signing.
 * <p>
 * It's constructed in accordance with the following environment variables:
 *  <ul>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_VERSION}: permitted values are "2.2"
 * </li>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_RPST}:
 * <p>
 * If this is an absolute path, then the filesystem-supplied resource
 * principal session token will be retrieved from that location. This mode
 * supports token refresh (if the environment replaces the RPST in the
 * filesystem). Otherwise, the environment variable is taken to hold the raw
 * value of an RPST. Under these circumstances, the RPST cannot be refreshed;
 * consequently, this mode is only usable for short-lived executables.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM}:
 * If this is an absolute path, then the filesystem-supplied private key will
 * be retrieved from that location. As with the OCI_RESOURCE_PRINCIPAL_RPST,
 * this mode supports token refresh if the environment can update the file
 * contents. Otherwise, the value is interpreted as the direct injection of a
 * private key. The same considerations as to the lifetime of this value apply
 * when directly injecting a key.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE}:
 * <p>
 * This is optional. If set, it contains either the location (as an absolute
 * path) or the value of the passphrase associated with the private key.
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_REGION}:
 * <p>
 * If set, this holds the canonical form of the local region. This is intended
 * to enable executables to locate their "local" OCI service endpoints.</p>
 * </li>
 * </ul>
 */
class ResourcePrincipalProvider
    implements AuthenticationProfileProvider, RegionProvider {

    /* Environment variable names used to fetch artifacts */
    private static final String OCI_RESOURCE_PRINCIPAL_VERSION =
        "OCI_RESOURCE_PRINCIPAL_VERSION";
    private static final String RP_VERSION_2_2 = "2.2";
    private static final String OCI_RESOURCE_PRINCIPAL_RPST =
        "OCI_RESOURCE_PRINCIPAL_RPST";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM =
        "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM";
    private static final String OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE =
        "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE";
    private static final String OCI_RESOURCE_PRINCIPAL_REGION =
        "OCI_RESOURCE_PRINCIPAL_REGION";

    private final ResourcePrincipalTokenSupplier tokenSupplier;
    private final DefaultSessionKeySupplier sessionKeySupplier;
    private final Region region;

    public static ResourcePrincipalProvider build(Logger logger) {
        if (logger == null) {
            logger = Logger.getLogger(ResourcePrincipalProvider.class.getName());
            logger.setLevel(Level.WARNING);
        }
        String version = System.getenv(OCI_RESOURCE_PRINCIPAL_VERSION);
        if (version == null) {
            throw new IllegalArgumentException(
                OCI_RESOURCE_PRINCIPAL_VERSION +
                " environment variable missing");
        }
        if (!version.equals(RP_VERSION_2_2)) {
            throw new IllegalArgumentException(
                OCI_RESOURCE_PRINCIPAL_VERSION +
                " has unknown value " + version);
        }

        String rpPrivateKey = System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM);
        String kp = System.getenv(OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);

        SessionKeyPairSupplier sessKeySupplier;
        if (rpPrivateKey == null) {
            throw new IllegalArgumentException(
                OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM +
                " environment variable missing");
        }

        if (new File(rpPrivateKey).isAbsolute()) {
            if (kp != null && !new File(kp).isAbsolute()) {
                throw new IllegalArgumentException(
                    "cannot mix path and constant settings for " +
                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM + " and " +
                    OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE);
            }
            sessKeySupplier = new FileKeyPairSupplier(rpPrivateKey, kp);
        } else {
            char[] passPhraseChars = null;
            if (kp != null) {
                passPhraseChars = kp.toCharArray();
            }
            sessKeySupplier = new FixedKeyPairSupplier(rpPrivateKey,
                                                       passPhraseChars);
        }

        String rpst = System.getenv(OCI_RESOURCE_PRINCIPAL_RPST);
        if (rpst == null) {
            throw new IllegalArgumentException(
                OCI_RESOURCE_PRINCIPAL_RPST + " environment variable missing");
        }
        ResourcePrincipalTokenSupplier tokenSupplier;
        if (new File(rpst).isAbsolute()) {
            tokenSupplier = new FileSecurityTokenSupplier(
                            sessKeySupplier, rpst, logger);
        } else {
            tokenSupplier = new FixedSecurityTokenSupplier(sessKeySupplier,
                                                           rpst);
        }

        String rpRegion = System.getenv(OCI_RESOURCE_PRINCIPAL_REGION);
        if (rpRegion == null) {
            throw new IllegalArgumentException(
                OCI_RESOURCE_PRINCIPAL_REGION + " environment variable missing");
        }
        Region r = Region.fromRegionId(rpRegion);
        return new ResourcePrincipalProvider(tokenSupplier, sessKeySupplier, r);
    }

    ResourcePrincipalProvider(ResourcePrincipalTokenSupplier tkSupplier,
                              SessionKeyPairSupplier keyPairSupplier,
                              Region region) {
        this.tokenSupplier = tkSupplier;
        this.sessionKeySupplier = new DefaultSessionKeySupplier(keyPairSupplier);
        this.region = region;
    }

    @Override
    public String getKeyId() {
        return "ST$" + tokenSupplier.getSecurityToken();
    }

    @Override
    public InputStream getPrivateKey() {
        return new ByteArrayInputStream(sessionKeySupplier.getPrivateKeyBytes());
    }

    @Override
    public char[] getPassphraseCharacters() {
        return null;
    }

    @Override
    public Region getRegion() {
        return region;
    }

    /**
     * @hidden
     * Internal only
     * <p>
     * Session tokens carry JWT claims. Permit the retrieval of the
     * value of those claims from the token.
     * @param key the name of a claim in the session token
     * @return the claim value.
     */
    String getClaim(String key) {
        return tokenSupplier.getStringClaim(key);
    }
}
