/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.ResourcePrincipalTokenSupplier.FileSecurityTokenSupplier;
import oracle.nosql.driver.iam.ResourcePrincipalTokenSupplier.FixedSecurityTokenSupplier;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FileKeyPairSupplier;
import oracle.nosql.driver.iam.SessionKeyPairSupplier.FixedKeyPairSupplier;
import oracle.nosql.driver.iam.SignatureProvider.ResourcePrincipalClaimKeys;
import oracle.nosql.driver.ops.GetRequest;

public class ResourcePrincipalProviderTest extends DriverTestBase {
    private static String TOKEN =
        "{" +
        "\"sub\": \"ocid1.resource.oc1.phx.resource\"," +
        "\"opc-certtype\": \"resource\"," +
        "\"iss\": \"authService.oracle.com\"," +
        "\"res_compartment\": \"compartmentId\"," +
        "\"res_tenant\": \"tenantId\"," +
        "\"fprint\": \"fingerprint\"," +
        "\"ptype\": \"instance\"," +
        "\"aud\": \"oci\"," +
        "\"opc-tag\": \"V1,ocid1.dynamicgroup.oc1..dgroup\"," +
        "\"ttype\": \"x509\"," +
        "\"opc-instance\": \"ocid1.instance.oc1.phx.instance\"," +
        "\"exp\": %s," +
        "\"opc-compartment\": \"TestTenant\"," +
        "\"iat\": 19888762000," +
        "\"jti\": \"jti\"," +
        "\"tenant\": \"TestTenant\"," +
        "\"jwk\": \"{\\\"kid\\\":\\\"kid\\\"," +
        "\\\"n\\\":\\\"%s\\\",\\\"e\\\":\\\"%s\\\"," +
        "\\\"kty\\\":\\\"RSA\\\",\\\"alg\\\":\\\"RS256\\\"," +
        "\\\"use\\\":\\\"sig\\\"}\"," +
        "\"opc-tenant\": \"TestTenant\"}";
    private static KeyPairInfo keypair;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        keypair = generateKeyPair();
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
    public void testKeyPairSupplier()
        throws Exception {

        String key = generatePrivateKeyFile("key.pem", null);
        SessionKeyPairSupplier supplier = new FileKeyPairSupplier(key, null);
        assertNotNull(supplier.getKeyPair());

        File passphraseFile = new File(getTestDir(), "passphrase");
        Files.write(passphraseFile.toPath(), "123456".getBytes());

        String encKey = generatePrivateKeyFile("enckey1.pem",
                                               "123456".toCharArray());
        supplier = new FileKeyPairSupplier(encKey,
                                           passphraseFile.getAbsolutePath());
        assertNotNull(supplier.getKeyPair());

        try {
            supplier = new FileKeyPairSupplier("non-existent", null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "exist");
        }

        try {
            supplier = new FileKeyPairSupplier(
                generatePrivateKeyFile("enckey2.pem", "123456".toCharArray()),
                "non-existent-passphrase");
            fail("expected");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage(), "Unable to read");
        }

        passphraseFile = new File(getTestDir(), "wrong-pass");
        Files.write(passphraseFile.toPath(), "1234".getBytes());
        try {
            supplier = new FileKeyPairSupplier(
                generatePrivateKeyFile("enckey3.pem", "123456".toCharArray()),
                passphraseFile.getAbsolutePath());
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "passphrase is incorrect");
        }

        String k = new String(Files.readAllBytes(new File(key).toPath()));
        supplier = new FixedKeyPairSupplier(k, null);
        assertNotNull(supplier.getKeyPair());

        k = new String(Files.readAllBytes(new File(encKey).toPath()));
        supplier = new FixedKeyPairSupplier(k, "123456".toCharArray());
        assertNotNull(supplier.getKeyPair());

        k = "abc";
        try {
            supplier = new FixedKeyPairSupplier(k, null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "in PEM format");
        }
    }

    @Test
    public void testResourcePrincipalTokenSupplier()
        throws Exception {

        String token = securityToken(TOKEN, keypair.getPublicKey());
        Path keyFile = Files.write(Paths.get(getTestDir(), "key.pem"),
                                   keypair.getKey().getBytes(),
                                   StandardOpenOption.CREATE);
        SessionKeyPairSupplier keySupplier = new FileKeyPairSupplier(
            keyFile.toAbsolutePath().toString(), null);
        ResourcePrincipalTokenSupplier supplier =
            new FixedSecurityTokenSupplier(keySupplier, token, null);
        assertEquals(supplier.getSecurityToken(), token);
        assertEquals(supplier.getStringClaim(
                     ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY),
                     "compartmentId");
        assertEquals(supplier.getStringClaim(
                     ResourcePrincipalClaimKeys.TENANT_ID_CLAIM_KEY),
                     "tenantId");

        File tokenFile = new File(getTestDir(), "token");
        Files.write(tokenFile.toPath(), token.getBytes());
        FileSecurityTokenSupplier fileSupplier =
            new FileSecurityTokenSupplier(keySupplier,
                                          tokenFile.getAbsolutePath(),
                                          null);
        SecurityToken st = fileSupplier.getSecurityTokenFromFile();
        assertEquals(st.getSecurityToken(), token);
        assertEquals(st.getStringClaim(
                     ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY),
                     "compartmentId");
        assertEquals(st.getStringClaim(
                     ResourcePrincipalClaimKeys.TENANT_ID_CLAIM_KEY),
                     "tenantId");
    }

    @Test
    public void testResourcePrincipal()
        throws Exception {

        String token = securityToken(TOKEN, keypair.getPublicKey());
        Path keyFile = Files.write(Paths.get(getTestDir(), "key.pem"),
                                   keypair.getKey().getBytes(),
                                   StandardOpenOption.CREATE);
        SessionKeyPairSupplier keySupplier = new FileKeyPairSupplier(
            keyFile.toAbsolutePath().toString(), null);
        ResourcePrincipalTokenSupplier tokenSupplier =
            new FixedSecurityTokenSupplier(keySupplier, token, null);
        ResourcePrincipalProvider rpProvider =
            new ResourcePrincipalProvider(tokenSupplier,
                                          keySupplier,
                                          Region.US_ASHBURN_1);

        assertNotNull(rpProvider.getKeyId());

        SignatureProvider provider = new SignatureProvider(rpProvider);
        NoSQLHandleConfig config = new NoSQLHandleConfig(provider);

        /* only need for test, NoSQLHandle will set implicitly for users */
        provider.prepare(config);

        GetRequest request = new GetRequest();
        String signature = provider.getAuthorizationString(request);
        assertNotNull(signature);

        assertEquals(provider.getResourcePrincipalClaim(
                     ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY),
                     "compartmentId");
        assertEquals(provider.getResourcePrincipalClaim(
                     ResourcePrincipalClaimKeys.TENANT_ID_CLAIM_KEY),
                     "tenantId");
    }
}
