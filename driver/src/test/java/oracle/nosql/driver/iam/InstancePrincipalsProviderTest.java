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

@SuppressWarnings("restriction")
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
        sp.setRequiredHeaders(authzString, request, headers, null/* content */);
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
