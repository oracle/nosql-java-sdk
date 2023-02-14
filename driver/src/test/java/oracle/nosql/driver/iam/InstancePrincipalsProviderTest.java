/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.KeyPair;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.FreePortLocator;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.iam.CertificateSupplier.DefaultCertificateSupplier;
import oracle.nosql.driver.iam.CertificateSupplier.URLResourceDetails;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableRequest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

public class InstancePrincipalsProviderTest extends DriverTestBase {
    private static final FreePortLocator portLocator =
        new FreePortLocator("localhost", 4242, 14000);
    private static final String tenantId = "TestTenant";
    private static HttpServer server;
    private static int port;
    private static String base;
    private static KeyPairInfo keypair;

    private static String TOKEN_PAYLOAD =
        "{" +
        "\"sub\": \"ocid1.instance.oc1.phx.instance\"," +
        "\"opc-certtype\": \"instance\"," +
        "\"iss\": \"authService.oracle.com\"," +
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

    /* security token response format */
    private static String TOKEN_RESPONSE = "{\"token\": \"%s\"}";
    private static boolean SERVER_ERROR = false;
    private static boolean EXPIRING_TOKEN = false;
    private static int REFRESH_WINDOW_SEC = 1;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        keypair = generateKeyPair();
        port = portLocator.next();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.start();
        base = "http://localhost:" + port;
        configHttpServer();
    }

    @AfterClass
    public static void staticTearDown() {
        server.stop(0);
    }

    @Before
    public void setUp() {
        SERVER_ERROR = false;
        EXPIRING_TOKEN = false;
    }

    private static void configHttpServer() {
        server.createContext("/instance", new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange)
                throws IOException {

                URI req = exchange.getRequestURI();
                String query = req.getQuery();
                if (query.contains("key.pem")) {
                    writeResponse(exchange, keypair.getKey());
                } else if (query.contains("cert.pem")) {
                    writeResponse(exchange, keypair.getCert());
                } else if (query.contains("intermediate.pem")){
                    writeResponse(exchange, keypair.getCert());
                } else if (query.contains("error.pem")){
                    writeResponse(exchange, "error");
                } else if (query.contains("ioe")){
                    throw new IOException("reading IOE");
                }
            }
        });

        server.createContext("/v1/x509", new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange)
                throws IOException {

                if (SERVER_ERROR) {
                    writeResponse(exchange,
                                  HttpURLConnection.HTTP_BAD_REQUEST,
                                  "error");
                } else if (EXPIRING_TOKEN) {
                    writeResponse(exchange, expiringTokenResponse());
                } else {
                    writeResponse(exchange, securityTokenResponse());
                }
            }
        });
    }

    private static String securityTokenResponse() {
        return String.format(TOKEN_RESPONSE, securityToken());
    }

    private static String securityToken() {
        return securityToken(TOKEN_PAYLOAD, keypair.getPublicKey());
    }

    private static String expiringTokenResponse() {
        return String.format(TOKEN_RESPONSE,
                             expiringToken(TOKEN_PAYLOAD,
                                           REFRESH_WINDOW_SEC,
                                           keypair.getPublicKey()));
    }

    private static URLResourceDetails getURLDetails(String url)
        throws Exception {

        return new URLResourceDetails(new URL(url));
    }

    @Test
    public void testBasic()
        throws Exception {

        CertificateSupplier leaf = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?cert.pem"),
            getURLDetails(base + "/instance?key.pem"),
            (char[]) null);

        CertificateSupplier inter = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?intermediate.pem"),
            null,
            (char[]) null);

        InstancePrincipalsProvider provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(leaf)
            .setIntermediateCertificateSuppliers(Collections.singleton(inter))
            .setTenantId(tenantId)
            .setSessionKeyPairSupplier(new TestSupplier(keypair.getKeyPair()))
            .build();
        provider.prepare(new NoSQLHandleConfig("http://test"));
        assertEquals("ST$" + securityToken(), provider.getKeyId());
    }

    @Test
    public void testDelegationToken()
        throws Exception {

        CertificateSupplier leaf = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?cert.pem"),
            getURLDetails(base + "/instance?key.pem"),
            (char[]) null);

        CertificateSupplier inter = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?intermediate.pem"),
            null,
            (char[]) null);

        InstancePrincipalsProvider provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(leaf)
            .setIntermediateCertificateSuppliers(Collections.singleton(inter))
            .setSessionKeyPairSupplier(new TestSupplier(keypair.getKeyPair()))
            .setTenantId(tenantId)
            .build();

        SignatureProvider sp = new SignatureProvider(provider);
        String delegationToken = "token-header.token-payload.token-sig";
        sp.setDelegationToken(delegationToken);
        sp.prepare(new NoSQLHandleConfig("http://test"));

        try {
            sp.setDelegationToken("bad-token");
            fail("expected iae");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("not in the valid"));
        }

        Request request = new TableRequest();
        request.setCompartmentInternal("compartment");
        String authzString = sp.getAuthorizationString(request);
        assertTrue(authzString.contains("opc-obo-token"));

        HttpHeaders headers = new DefaultHttpHeaders();
        sp.setRequiredHeaders(authzString, request, headers);
        assertEquals(headers.get("opc-obo-token"), delegationToken);
    }

    @Test
    public void testDefaultCertificateSupplier()
        throws Exception {

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?cert.pem"),
                getURLDetails(base + "/instance?error.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "Private key must be in PEM");
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?error.pem"),
                getURLDetails(base + "/instance?key.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "Invalid certificate");
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?cert.pem"),
                getURLDetails(base + "/instance?ioe.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "Unable to read private key");
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?ioe.pem"),
                getURLDetails(base + "/instance?key.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "Unable to read certificate");
        }
    }

    @Test
    public void testNoCertificates()
        throws Exception {

        CertificateSupplier noKeyPair = new CertificateSupplier() {

            @Override
            public X509CertificateKeyPair getCertificateAndKeyPair() {
                return null;
            }
        };

        InstancePrincipalsProvider provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(noKeyPair)
            .setIntermediateCertificateSuppliers(
                Collections.singleton(noKeyPair))
            .setTenantId(tenantId)
            .build();

        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "key pair not present");
        }

        CertificateSupplier noCert = new CertificateSupplier() {

            @Override
            public X509CertificateKeyPair getCertificateAndKeyPair() {
                return new X509CertificateKeyPair("", null, null);
            }
        };

        provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(noCert)
            .setIntermediateCertificateSuppliers(
                Collections.singleton(noCert))
            .setTenantId(tenantId)
            .build();

        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "certificate not present");
        }

        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        X509Certificate cer = (X509Certificate)fact.generateCertificate(
            new ByteArrayInputStream(keypair.getCert().getBytes()));
        CertificateSupplier noKey = new CertificateSupplier() {

            @Override
            public X509CertificateKeyPair getCertificateAndKeyPair() {
                return new X509CertificateKeyPair("", cer, null);
            }
        };

        provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(noKey)
            .setIntermediateCertificateSuppliers(
                Collections.singleton(noKey))
            .setTenantId(tenantId)
            .build();

        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "key not present");
        }
    }

    @Test
    public void testGetTokenError()
        throws Exception {

        SERVER_ERROR = true;

        CertificateSupplier leaf = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?cert.pem"),
            getURLDetails(base + "/instance?key.pem"),
            (char[]) null);

        CertificateSupplier inter = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?intermediate.pem"),
            null,
            (char[]) null);

        InstancePrincipalsProvider provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(leaf)
            .setIntermediateCertificateSuppliers(Collections.singleton(inter))
            .setTenantId(tenantId)
            .build();
        provider.prepare(new NoSQLHandleConfig("http://test"));

        try {
            provider.getKeyId();
        } catch (SecurityInfoNotReadyException iae) {
            assertThat(iae.getMessage(), "Error getting security token");
        }
    }

    @Test
    public void testValidateKey()
        throws Exception {

        EXPIRING_TOKEN = true;

        CertificateSupplier leaf = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?cert.pem"),
            getURLDetails(base + "/instance?key.pem"),
            (char[]) null);

        CertificateSupplier inter = new DefaultCertificateSupplier(
            getURLDetails(base + "/instance?intermediate.pem"),
            null,
            (char[]) null);

        InstancePrincipalsProvider provider =
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(base)
            .setLeafCertificateSupplier(leaf)
            .setIntermediateCertificateSuppliers(Collections.singleton(inter))
            .setTenantId(tenantId)
            .build();
        provider.setMinTokenLifetime(REFRESH_WINDOW_SEC * 1000 + 100);
        provider.prepare(new NoSQLHandleConfig("http://test"));
        try {
            provider.getKeyId();
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), "less lifetime");
        }
    }

    @Test
    public void testRegionURI() {
        for (Region r : Region.getOC1Regions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclecloud.com");
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() + ".oraclecloud.com");
        }
        for (Region r : Region.getGovRegions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclegovcloud.com");
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() +
                         ".oraclegovcloud.com");
        }
        for (Region r : Region.getOC4Regions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclegovcloud.uk");
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() +
                         ".oraclegovcloud.uk");
        }
        for (Region r : Region.getOC8Regions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclecloud8.com");
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() +
                         ".oraclecloud8.com");
        }

        /* test IAM URI by airport code */
        assertEquals(Utils.getIAMURL("jnb"),
                    "https://auth.af-johannesburg-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("bom"),
                    "https://auth.ap-mumbai-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("sin"),
                    "https://auth.ap-singapore-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("icn"),
                    "https://auth.ap-seoul-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("syd"),
                    "https://auth.ap-sydney-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("nrt"),
                    "https://auth.ap-tokyo-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("mel"),
                    "https://auth.ap-melbourne-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("kix"),
                    "https://auth.ap-osaka-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("hyd"),
                    "https://auth.ap-hyderabad-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("yny"),
                    "https://auth.ap-chuncheon-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("mrs"),
                    "https://auth.eu-marseille-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("arn"),
                    "https://auth.eu-stockholm-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("fra"),
                    "https://auth.eu-frankfurt-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("zrh"),
                    "https://auth.eu-zurich-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("lhr"),
                    "https://auth.uk-london-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("ams"),
                    "https://auth.eu-amsterdam-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("auh"),
                    "https://auth.me-abudhabi-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("jed"),
                    "https://auth.me-jeddah-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("dxb"),
                    "https://auth.me-dubai-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("cwl"),
                    "https://auth.uk-cardiff-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("iad"),
                    "https://auth.us-ashburn-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("phx"),
                    "https://auth.us-phoenix-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("sjc"),
                    "https://auth.us-sanjose-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("yyz"),
                    "https://auth.ca-toronto-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("yul"),
                    "https://auth.ca-montreal-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("gru"),
                    "https://auth.sa-saopaulo-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("scl"),
                    "https://auth.sa-santiago-1.oraclecloud.com");

        assertEquals(Utils.getIAMURL("lfi"),
                    "https://auth.us-langley-1.oraclegovcloud.com");
        assertEquals(Utils.getIAMURL("luf"),
                    "https://auth.us-luke-1.oraclegovcloud.com");

        assertEquals(Utils.getIAMURL("ric"),
                    "https://auth.us-gov-ashburn-1.oraclegovcloud.com");
        assertEquals(Utils.getIAMURL("pia"),
                    "https://auth.us-gov-chicago-1.oraclegovcloud.com");
        assertEquals(Utils.getIAMURL("tus"),
                    "https://auth.us-gov-phoenix-1.oraclegovcloud.com");

        assertEquals(Utils.getIAMURL("ltn"),
                    "https://auth.uk-gov-london-1.oraclegovcloud.uk");

        assertEquals(Utils.getIAMURL("nja"),
                     "https://auth.ap-chiyoda-1.oraclecloud8.com");

        assertEquals(Utils.getIAMURL("mct"),
                     "https://auth.me-dcc-muscat-1.oraclecloud9.com");

        assertEquals(Utils.getIAMURL("wga"),
                     "https://auth.ap-dcc-canberra-1.oraclecloud10.com");
    }

    private static class TestSupplier implements SessionKeyPairSupplier {
        final private KeyPair keyPair;

        TestSupplier(KeyPair kp) {
            this.keyPair = kp;
        }

        @Override
        public KeyPair getKeyPair() {
            return keyPair;
        }

        @Override
        public void refreshKeys() {
        }
    }
}
