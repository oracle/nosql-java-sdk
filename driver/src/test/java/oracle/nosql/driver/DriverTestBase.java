/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Base64.Encoder;

import oracle.nosql.driver.iam.pki.Pem;
import com.sun.net.httpserver.HttpExchange;

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

        File keyFile = new File(getTestDir(), name);
        if (!keyFile.exists()) {
            keyFile.createNewFile();
        }
        try (WritableByteChannel pem = Files.newByteChannel(
                keyFile.toPath(), StandardOpenOption.WRITE)) {
            Pem.Encoder encoder = (passphrase == null) ?
                Pem.encoder().with(Pem.Format.LEGACY) :
                Pem.encoder().with(Pem.Format.LEGACY)
                    .with(Pem.Passphrase.of(passphrase));

            encoder.write(pem, keypair.getPrivate());
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

        final File keyStoreFile = new File(getTestDir(), "keystore");
        final String pwd = "123456";
        final String alias = "test";

        String[] keyStoreCmds = new String[] {
            "keytool",
            "-genkeypair",
            "-keystore", keyStoreFile.getAbsolutePath(),
            "-storetype", "PKCS12",
            "-storepass", pwd,
            "-keypass", pwd,
            "-alias", alias,
            "-dname", "OU=opc-tenant:TestTenant",
            "-keyAlg", "RSA",
            "-keysize", 2048 + "",
            "-validity", 365 + ""};

        Process proc = Runtime.getRuntime().exec(keyStoreCmds);
        boolean done = false;
        int returnCode = 0;
        while (!done) {
            returnCode = proc.waitFor();
            done = true;
        }

        if (returnCode != 0) {
            throw new IllegalStateException(
                "Error generating keystore:" + returnCode);
        }

        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(new FileInputStream(keyStoreFile), pwd.toCharArray());
        KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry)
            keystore.getEntry(
                alias, new KeyStore.PasswordProtection(pwd.toCharArray()));
        PrivateKey pk = entry.getPrivateKey();
        Certificate cert = entry.getCertificate();
        KeyPair keyPair = new KeyPair(cert.getPublicKey(), pk);

        return new KeyPairInfo(
            new String(Pem.encoder().with(Pem.Format.LEGACY).encode(pk)),
            Pem.encoder().with(Pem.Format.LEGACY).encode(cert),
            keyPair);
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
