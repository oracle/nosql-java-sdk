/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URL;

import oracle.nosql.driver.ops.GetRequest;

import org.junit.Test;

/**
 * Basic handle configuration sanity checks
 */
public class HandleConfigTest {
    @Test
    public void testEndpoints() {

        /* A prod endpoint defaults to https protocol and port 443 */
        validate("ndcs.uscom-east-foo.oraclecloud.com",
                 "https",
                 "ndcs.uscom-east-foo.oraclecloud.com",
                 443);

        validate(Region.AP_MUMBAI_1,
                 "https",
                 "nosql.ap-mumbai-1.oci.oraclecloud.com",
                 443);

        validate(Region.US_ASHBURN_1,
                 "https",
                 "nosql.us-ashburn-1.oci.oraclecloud.com",
                 443);

        validate("us-AshBURN-1",
                 "https",
                 "nosql.us-ashburn-1.oci.oraclecloud.com",
                 443);

        /*
         * this is treated as a simple nosql endpoint. It will only
         * fail when trying to connect to the service.
         */
        validate("not-a-region",
                 "https",
                 "not-a-region",
                 443);

        validate("https://foobar-east.oracle.com",
                "https", "foobar-east.oracle.com", 443);
        validate("foobar-east.oracle.com",
                "https", "foobar-east.oracle.com", 443);
        validate("foobar-east.oracle.com:443",
                "https", "foobar-east.oracle.com", 443);
       validate("foobar-east.oracle.com:13000",
                 "http",  "foobar-east.oracle.com", 13000);

        validate("localhost:8080", "http", "localhost", 8080);
        validate("http://localhost:123", "http", "localhost", 123);
        validate("http://localhost", "http", "localhost", 8080);

        validate("HtTp://localhost", "http", "localhost", 8080);
        validate("HtTpS://Foo.com:90", "https", "Foo.com", 90);

        /*
         * We catch invalid port numbers, too many parts, but we don't
         * check whether the protocol and ports make sense together.
         */
        expectIllegalArg("http://foobar.east.oracle.com:-10");
        expectIllegalArg("http://foobar.east.oracle.com:80:10");
        expectIllegalArg("httpxx://foobar.east.oracle.com");
    }

    @Test
    public void testSslProtocols() {
        NoSQLHandleConfig config = new NoSQLHandleConfig("http://foo.com");
        config.setSSLProtocols("TLSv1.3", "TLSv1.2");
        assertEquals(2, config.getSSLProtocols().length);

        try {
            config.setSSLProtocols("TLSv1.4");
        } catch(IllegalArgumentException expected) {
            // expect a failure
        }
        try {
            config.setSSLProtocols("TLSv1.4,");
        } catch(IllegalArgumentException expected) {
            // expect a failure
        }
    }

    @Test
    public void testSdkVersion() {
        final String version = NoSQLHandleConfig.getLibraryVersion();
        assertNotNull("SDK version should be non-null", version);
        /* check that version string has three dot-separated parts */
        String[] arr = version.split("\\.", -1);
        assertTrue("SDK version \"" + version +
                   "\" should have at least three dot-separated parts",
                   arr.length > 2);
    }

    @Test
    public void testHttpHeaderValueValidation() {
        NoSQLHandleConfig config = new NoSQLHandleConfig("http://foo.com");
        GetRequest request = new GetRequest();

        config.setDefaultCompartment("compartment.a");
        assertEquals("compartment.a", config.getDefaultCompartment());
        config.setDefaultNamespace("namespace_a");
        assertEquals("namespace_a", config.getDefaultNamespace());
        config.setExtensionUserAgent("app plugin/1.0");
        assertEquals("app plugin/1.0", config.getExtensionUserAgent());
        request.setCompartment("compartment.b");
        assertEquals("compartment.b", request.getCompartment());
        request.setNamespace("namespace_b");
        assertEquals("namespace_b", request.getNamespace());

        expectIllegalValue(() ->
            config.setExtensionUserAgent("safe\r\nX-Injected: yes"));
        expectIllegalValue(() ->
            config.setExtensionUserAgent("safe\tunsafe"));
        expectIllegalValue(() ->
            config.setDefaultCompartment("compartment\nX-Injected: yes"));
        expectIllegalValue(() ->
            config.setDefaultNamespace("namespace\u0000unsafe"));
        expectIllegalValue(() ->
            request.setCompartment("compartment\rX-Injected: yes"));
        expectIllegalValue(() ->
            request.setNamespace("namespace\u007funsafe"));

        config.setDefaultCompartment(null);
        assertNull(config.getDefaultCompartment());
        config.setDefaultNamespace(null);
        assertNull(config.getDefaultNamespace());
        config.setExtensionUserAgent(null);
        assertNull(config.getExtensionUserAgent());
        request.setCompartment(null);
        assertNull(request.getCompartment());
        request.setNamespace(null);
        assertNull(request.getNamespace());
    }

    private void expectIllegalArg(String endpoint) {
        try {
            new NoSQLHandleConfig(endpoint);
            fail("Endpoint " + endpoint + " should have failed");
        } catch(IllegalArgumentException expected) {
            // expect a failure
        }

    }

    private void expectIllegalValue(Runnable setter) {
        try {
            setter.run();
            fail("Value should have failed validation");
        } catch(IllegalArgumentException expected) {
            // expect a failure
        }
    }

    private void validate(String endpoint, String protocol,
                          String host, int port) {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        URL configURL = config.getServiceURL();
        assertEquals(protocol, configURL.getProtocol());
        assertEquals(host, configURL.getHost());
        assertEquals(port, configURL.getPort());
    }

    private void validate(Region region, String protocol,
                          String host, int port) {
        NoSQLHandleConfig config = new NoSQLHandleConfig(region, null);
        URL configURL = config.getServiceURL();
        assertEquals(protocol, configURL.getProtocol());
        assertEquals(host, configURL.getHost());
        assertEquals(port, configURL.getPort());
    }
}
