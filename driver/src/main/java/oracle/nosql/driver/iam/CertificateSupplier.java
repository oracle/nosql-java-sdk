/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Refreshable;

/**
 * @hidden
 * Internal use only
 * <p>
 * The X509 certficate and private key pair supplier. It supplies key pair
 * to SecurityTokenSupplier which will be used to obtain JWT token from IAM.
 */
interface CertificateSupplier {

    /**
     * Returns the X509 certificate and private key.  The X509 certificate will
     * always be valid.  The private key may be null for intermediate
     * certificates.  For leaf certificates, the private key will always be
     * valid.
     *
     * @return The X509 certificate and private key pair.
     */
    X509CertificateKeyPair getCertificateAndKeyPair();

    /**
     * @hidden
     */
    static class X509CertificateKeyPair {
        private final X509Certificate certificate;
        private final String rawCertificate;
        private final RSAPrivateKey privateKey;

        public X509CertificateKeyPair(String rawCert,
                                      X509Certificate cert,
                                      RSAPrivateKey key) {
            this.rawCertificate = rawCert;
            this.certificate = cert;
            this.privateKey = key;
        }

        public String getRawCertificate() {
            return rawCertificate;
        }

        public X509Certificate getCertificate() {
            return certificate;
        }

        public RSAPrivateKey getKey() {
            return privateKey;
        }
    }

    /**
     * @hidden
     * The default X509 certificate supplier implementation that reads private
     * key and certificate from given URL.
     */
    static class DefaultCertificateSupplier
        implements CertificateSupplier, Refreshable {

        /**
         * The certificate and the private key of certificate.
         */
        private final AtomicReference<X509CertificateKeyPair> keyPair =
            new AtomicReference<>(null);

        /**
         * The url of certificate.
         */
        private final URLResourceDetails certificateUrl;

        /**
         * The url of private key.
         */
        private final URLResourceDetails privateKeyUrl;

        /**
         * The passphrase of private key.
         */
        private final char[] keyPass;

        DefaultCertificateSupplier(URLResourceDetails certificateUrl,
                                   URLResourceDetails privateKeyUrl,
                                   char[] keyPassphrase) {
            this.certificateUrl = certificateUrl;
            this.privateKeyUrl = privateKeyUrl;
            this.keyPass = keyPassphrase;

            refresh();
        }

        @Override
        public X509CertificateKeyPair getCertificateAndKeyPair() {
            return keyPair.get();
        }

        @Override
        public boolean isCurrent() {
            return false;
        }

        @Override
        public void refresh() {
            String rawCertificate = readRawCertificate(certificateUrl);
            X509Certificate certificate = readCertificate(rawCertificate);
            RSAPrivateKey key = readPrivateKey(privateKeyUrl, keyPass);
            this.keyPair.set(new X509CertificateKeyPair(
                             rawCertificate, certificate, key));
        }

        /**
         * Read the certificate from a raw string.
         * @param certificate the certificate
         * @return the certificate
         */
        private static X509Certificate readCertificate(String certificate) {
            try {
                CertificateFactory factory =
                    CertificateFactory.getInstance("X.509");
                return (X509Certificate) factory.generateCertificate(
                    new ByteArrayInputStream(certificate.getBytes()));
            } catch (CertificateException e) {
                throw new IllegalArgumentException("Invalid certificate.", e);
            }
        }

        private static String readRawCertificate(URLResourceDetails certURL) {
            try (InputStream is = getResourceStream(certURL)) {
                return Utils.readStream(is);
            } catch (IOException e) {
                throw new IllegalArgumentException(
                    "Unable to read certificate from " + certURL.getURL(), e);
            }
        }

        /**
         * Read the private key from url.
         * @param privateKeyUrl the private key url.
         * @param privateKeyPassphrase the private key passhprase
         * @return the private key
         */
        private static RSAPrivateKey readPrivateKey(URLResourceDetails keyURL,
                                                    char[] keyPassphrase) {
            if (keyURL == null) {
                return null;
            }

            try {
                return new PrivateKeyProvider(getResourceStream(keyURL),
                                              keyPassphrase)
                    .getKey();
            } catch (IOException e) {
                throw new IllegalArgumentException(
                    "Unable to read private key from " + keyURL.getURL(), e);
            }
        }

        private static InputStream getResourceStream(URLResourceDetails urd)
                throws IOException {
            final URLConnection urlConnection = urd.getURL().openConnection();
            if (urd.getURLHeaders() != null) {
                urd.getURLHeaders().forEach(urlConnection::setRequestProperty);
            }
            return urlConnection.getInputStream();
        }
    }

    static class URLResourceDetails {
        private URL url;
        private Map<String, String> headers;

        URLResourceDetails(URL url) {
            this.url = url;
        }

        URL getURL() {
            return this.url;
        }

        URLResourceDetails addHeader(String name, String value) {
            if (headers == null) {
                headers = new HashMap<>();
            }
            headers.put(name, value);
            return this;
        }

        Map<String, String> getURLHeaders() {
            return headers;
        }
    }
}
