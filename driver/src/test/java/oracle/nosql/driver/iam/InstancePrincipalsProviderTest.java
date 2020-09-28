/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;

import oracle.nosql.driver.DriverTestBase;
import oracle.nosql.driver.FreePortLocator;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.CertificateSupplier.DefaultCertificateSupplier;
import oracle.nosql.driver.iam.CertificateSupplier.URLResourceDetails;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

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
        "\"exp\": 19888762000," +
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
                }
                writeResponse(exchange, securityTokenResponse());
            }
        });
    }

    private static String securityTokenResponse() {

        return String.format(TOKEN_RESPONSE, securityToken());
    }

    private static String securityToken() {
        return securityToken(TOKEN_PAYLOAD, keypair.getPublicKey());
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
            .build();
        assertEquals("ST$" + securityToken(), provider.getKeyId());
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
            assertThat("error private key", iae.getMessage(),
                       containsString("Private key must be in PEM"));
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?error.pem"),
                getURLDetails(base + "/instance?key.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat("invalid certificate", iae.getMessage(),
                       containsString("Invalid certificate"));
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?cert.pem"),
                getURLDetails(base + "/instance?ioe.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat("unable to read", iae.getMessage(),
                       containsString("Unable to read private key"));
        }

        try {
            new DefaultCertificateSupplier(
                getURLDetails(base + "/instance?ioe.pem"),
                getURLDetails(base + "/instance?key.pem"),
                (char[]) null);
            fail("expected");
        } catch (IllegalArgumentException iae) {
            assertThat("unable to read", iae.getMessage(),
                       containsString("Unable to read certificate"));
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
        } catch (NullPointerException npe) {
            assertThat("no key pair", npe.getMessage(),
                       containsString("key pair not present"));
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
        } catch (NullPointerException npe) {
            assertThat("no cert", npe.getMessage(),
                       containsString("certificate not present"));
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
        } catch (NullPointerException npe) {
            assertThat("no cert", npe.getMessage(),
                       containsString("key not present"));
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

        try {
            provider.getKeyId();
        } catch (IllegalArgumentException iae) {
            assertThat("error getting token", iae.getMessage(),
                       containsString("Failed to get"));
        }
    }

    @Test
    public void testRegionURI() {
        for (Region r : Region.getOC1Regions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclecloud.com");
        }
        for (Region r : Region.getOC1Regions()) {
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() + ".oraclecloud.com");
        }
        for (Region r : Region.getGovRegions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclegovcloud.com");
        }
        for (Region r : Region.getOC4Regions()) {
            assertEquals(r.endpoint(),
                         "https://nosql." + r.getRegionId() +
                         ".oci.oraclegovcloud.uk");
        }
        for (Region r : Region.getGovRegions()) {
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() +
                         ".oraclegovcloud.com");
        }
        for (Region r : Region.getOC4Regions()) {
            assertEquals(Utils.getIAMURL(r.getRegionId()),
                         "https://auth." + r.getRegionId() +
                         ".oraclegovcloud.uk");
        }

        /* test IAM URI by airport code */
        assertEquals(Utils.getIAMURL("bom"),
                    "https://auth.ap-mumbai-1.oraclecloud.com");
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

        assertEquals(Utils.getIAMURL("fra"),
                    "https://auth.eu-frankfurt-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("zrh"),
                    "https://auth.eu-zurich-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("lhr"),
                    "https://auth.uk-london-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("ams"),
                    "https://auth.eu-amsterdam-1.oraclecloud.com");
        assertEquals(Utils.getIAMURL("jed"),
                    "https://auth.me-jeddah-1.oraclecloud.com");

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
    }
}
