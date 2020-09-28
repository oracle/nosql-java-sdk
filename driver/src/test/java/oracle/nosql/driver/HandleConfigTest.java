/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URL;

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
        config.setSSLProtocols("TLSv1.3", "TLSv1.1");
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

    private void expectIllegalArg(String endpoint) {
        try {
            new NoSQLHandleConfig(endpoint);
            fail("Endpoint " + endpoint + " should have failed");
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
