/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * Utility methods for creating Netty SSL contexts.
 */
public final class NettySslContextUtil {

    private NettySslContextUtil() {
    }

    /**
     * Creates a client SSL context builder that prefers the Netty OpenSSL
     * provider when tcnative is available, falling back to the JDK provider
     * otherwise.
     */
    public static SslContextBuilder newClientContextBuilder() {
        SslContextBuilder builder = SslContextBuilder.forClient();
        if (OpenSsl.isAvailable()) {
            builder.sslProvider(SslProvider.OPENSSL);
        }
        return builder;
    }
}
