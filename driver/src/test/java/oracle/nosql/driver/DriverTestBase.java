/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Provider;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.interfaces.RSAPublicKey;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Date;

import com.sun.net.httpserver.HttpExchange;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptor;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * A common base for driver tests. It is empty at this point but may
 * eventually contain common utilities.
 */
@SuppressWarnings("restriction")
public class DriverTestBase {

    /*
     * Returns a path to the file in the resources dir (in mvn it's
     * target/test-classes)
     */
    protected static String getResourcePath(String resource) {
        String res = "/" + resource;

        if (DriverTestBase.class.getResource(res) != null) {
            return DriverTestBase.class.getResource(res).getFile();
        }
        return null;
    }

    /*
     * In mvn the test dir is in the pom.xml file in the
     * config for the maven-surefire-plugin that runs junit
     * -- target/test-run
     */
    protected static String getTestDir() {
        return ".";
    }

    /*
     * Tests are run from driver/target/test-run, return the relative path
     * to the test resources directory
     */
    protected static String getResourcesDir() {
        return "../../src/test/resources/";
    }

    protected static void clearTestDirectory() {
        File testDir = new File(getTestDir());
        if (!testDir.exists()) {
            return;
        }
        clearDirectory(testDir);
    }

    private static void clearDirectory(File dir) {
        if (dir.listFiles() == null) {
            return;
        }
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                clearDirectory(file);
            }
            boolean deleteDone = file.delete();
            assert deleteDone: "Couldn't delete " + file;
        }
    }

    public static void writeResponse(HttpExchange exchange, String msg)
        throws IOException {

        writeResponse(exchange, HttpURLConnection.HTTP_OK, msg);
    }

    public static void writeResponse(HttpExchange exchange,
                                     int status,
                                     String msg)
        throws IOException {

        exchange.getResponseHeaders().set("Content-Type",
                                          "application/json");
        exchange.sendResponseHeaders(status, msg.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(msg.getBytes());
        }
    }

    protected static String securityToken(String payload, PublicKey publicKey) {
        return securityToken(payload, "19888762000", publicKey);
    }

    protected static String expiringToken(String payload,
                                          int expirySec,
                                          PublicKey publicKey) {
        long now = System.currentTimeMillis() / 1000;
        return securityToken(payload, "" + (now + expirySec), publicKey);
    }

    protected static String securityToken(String payload,
                                          String expirySec,
                                          PublicKey publicKey) {
        String header = "{\"kid\": \"asw_oc1_2019-06-27\",\"alg\": \"RS256\"}";
        String signature = "pseudo-signature";
        Encoder encoder = Base64.getUrlEncoder();
        RSAPublicKey pk = (RSAPublicKey)publicKey;
        String tokenPayload = String.format(
            payload,
            expirySec,
            encoder.encodeToString(pk.getModulus().toByteArray()),
            encoder.encodeToString(pk.getPublicExponent().toByteArray()));

        return encoder.encodeToString(header.getBytes()) + "." +
               encoder.encodeToString(tokenPayload.getBytes()) + "." +
               encoder.encodeToString(signature.getBytes());
    }

    /**
     * Generate a private key file.
     * @param name name of key file
     * @param passphrase passphrase if need encrypted key
     * @return absolute path of key file
     */
    protected static String generatePrivateKeyFile(String name,
                                                   char[] passphrase)
        throws Exception {

        KeyPairGenerator keygen = KeyPairGenerator.getInstance("RSA");
        keygen.initialize(2048);
        KeyPair keypair = keygen.generateKeyPair();
        PEMEncryptor pemEncryptor = null;

        if (passphrase != null) {
            JcePEMEncryptorBuilder builder =
                new JcePEMEncryptorBuilder("DES-EDE3-CBC");
            builder.setSecureRandom(new SecureRandom());
            builder.setProvider(getBouncyCastleProvider());
            pemEncryptor = builder.build(passphrase);
        }

        File keyFile = new File(getTestDir(), name);
        try (FileWriter privateWrite = new FileWriter(keyFile);
            JcaPEMWriter privatePemWriter = new JcaPEMWriter(privateWrite); ) {

            if (pemEncryptor != null) {
                privatePemWriter
                    .writeObject(keypair.getPrivate(), pemEncryptor);
            }
            else {
                privatePemWriter.writeObject(keypair.getPrivate());
            }
        }
        return keyFile.getAbsolutePath();
    }

    /**
     * Generate a RAS key and certificate, return in PEM. Note that certificate
     * must has OU with opc-tenant:TestTenant, because it's used by instance
     * and resource principal testing.
     * @return a string that the first element is key and the second one is
     * certificate.
     */
    protected static KeyPairInfo generateKeyPair()
        throws Exception {

        KeyPairGenerator keygen = KeyPairGenerator.getInstance("RSA");
        keygen.initialize(2048);
        KeyPair keypair = keygen.generateKeyPair();

        JcaPKCS8Generator gen = new JcaPKCS8Generator(keypair.getPrivate(),
                                                      null);
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw)) {
            pw.writeObject(gen.generate());
        }

        String key = sw.toString();

        X500Name name = new X500Name("OU=opc-tenant:TestTenant");
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo
            .getInstance(keypair.getPublic().getEncoded());
        Date start = new Date();
        Date until = Date.from(LocalDate.now().plus(3650, ChronoUnit.DAYS)
                         .atStartOfDay().toInstant(ZoneOffset.UTC));
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
            name,
            new BigInteger(10, new SecureRandom()),
            start,
            until,
            name,
            subPubKeyInfo
        );
        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA")
            .setProvider(new BouncyCastleProvider())
            .build(keypair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);

        Certificate cert = new JcaX509CertificateConverter()
            .setProvider(new BouncyCastleProvider()).getCertificate(holder);

        sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw)) {
            pw.writeObject(cert);
        }
        String certString = sw.toString();

        return new KeyPairInfo(key, certString, keypair);
    }

    /**
     * Assert whether given content contains expected string. The assertion is
     * case insensitive.
     * @param content content to examine
     * @param expected expected string
     */
    protected static void assertThat(String content, String expected) {
        if (!content.toLowerCase().contains(expected.toLowerCase())) {
            throw new IllegalArgumentException(
                content + " doesn't contains " + expected);
        }
    }

    private static Provider getBouncyCastleProvider()
        throws Exception {

        Class<?> providerClass = Class.forName(
            "org.bouncycastle.jce.provider.BouncyCastleProvider");
        return (Provider)providerClass
            .getDeclaredConstructor().newInstance();
    }

    protected static class KeyPairInfo {
        private String key;
        private String cert;
        private KeyPair keyPair;

        KeyPairInfo(String key, String cert, KeyPair keyPair) {
            this.key = key;
            this.cert = cert;
            this.keyPair = keyPair;
        }

        public String getKey() {
            return key;
        }

        public String getCert() {
            return cert;
        }

        public KeyPair getKeyPair() {
            return keyPair;
        }

        public PublicKey getPublicKey() {
            return keyPair.getPublic();
        }
    }
}
