/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.RSAPublicKeySpec;

/**
 * @hidden
 * Internal use only
 * <p>
 * A session key pair supplier is responsible for providing public/private key
 * pairs that will be used to both fetch federated service security tokens and
 * to sign individual request to OCI.
 * <p>
 * The returned keys should not change unless there as been a call refreshKeys().
 */
interface SessionKeyPairSupplier {

    /**
     * Returns the current RSA key pair.
     * @return The RSA key pair.
     */
    KeyPair getKeyPair();

    /**
     * If the IAM-issued security token expires, the token supplier
     * will try to re-generate a new set of keys by calling this method. The
     * implementation should create a new pair of keys for security reasons.
     *
     * Refreshing keys should not be a long-running blocking call. You can
     * refresh keys in an async thread and return from this method immediately.
     * When the async process is done refreshing the keys, the client code will
     * automatically pick up the latest set of keys and update the security
     * token accordingly
     */
    void refreshKeys();

    /**
     * @hidden
     * Helper class to cache the private key as bytes in order to avoid parsing
     * every time. The key only changes during calls to refresh.
     * <p>
     * All methods in this class that are called outside of this class should
     * be synchronized.
     */
    static class DefaultSessionKeySupplier implements SessionKeyPairSupplier {

        private final SessionKeyPairSupplier delegate;
        private RSAPrivateKey lastPrivateKey = null;
        private byte[] privateKeyBytes = null;

        protected DefaultSessionKeySupplier(SessionKeyPairSupplier delegate) {
            this.delegate = delegate;
            this.setPrivateKeyBytes(
                (RSAPrivateKey) delegate.getKeyPair().getPrivate());
        }

        @Override
        public KeyPair getKeyPair() {
            return delegate.getKeyPair();
        }

        @Override
        public synchronized void refreshKeys() {
            delegate.refreshKeys();
        }

        protected synchronized byte[] getPrivateKeyBytes() {
            /*
             * private keys can be refreshed asynchronously,
             * always update first
             */
            setPrivateKeyBytes((RSAPrivateKey) this.getKeyPair().getPrivate());
            return this.privateKeyBytes;
        }

        private void setPrivateKeyBytes(RSAPrivateKey privateKey) {
            /* quick shallow ref check only */
            if (privateKey != null && privateKey != lastPrivateKey) {
                lastPrivateKey = privateKey;
                this.privateKeyBytes = Utils.toByteArray(privateKey);
            }
        }
    }

    /**
     * @hidden
     * This is a helper class to generate in-memory temporary session keys using
     * JDK KeyPairGenerator.
     *
     * The thread safety of this class is ensured through the Caching class
     * above which synchronizes on all methods.
     */
    static class JDKKeyPairSupplier implements SessionKeyPairSupplier {
        private final static KeyPairGenerator GENERATOR;
        private KeyPair keyPair = null;

        static {
            try {
                GENERATOR = KeyPairGenerator.getInstance("RSA");
                GENERATOR.initialize(2048);
            } catch (NoSuchAlgorithmException e) {
                throw new Error(e.getMessage());
            }
        }

        protected JDKKeyPairSupplier() {
            this.keyPair = GENERATOR.generateKeyPair();
        }

        @Override
        public KeyPair getKeyPair() {
            return keyPair;
        }

        @Override
        public void refreshKeys() {
            this.keyPair = GENERATOR.generateKeyPair();
        }
    }

    /**
     * @hidden
     * This is a helper class to get key pair by given private key path.
     */
    static class FileKeyPairSupplier implements SessionKeyPairSupplier {
        private final String privateKeyPath;
        private final Path passphrasePath;
        private KeyPair keyPair = null;

        FileKeyPairSupplier(String privateKeyPath, String passphrasePath) {
            this.privateKeyPath = privateKeyPath;
            if (passphrasePath != null) {
                this.passphrasePath = new File(passphrasePath).toPath();
            } else {
                this.passphrasePath = null;
            }
            refreshKeys();
        }

        @Override
        public KeyPair getKeyPair() {
            return keyPair;
        }

        @Override
        public void refreshKeys() {
            if (privateKeyPath == null) {
                throw new IllegalArgumentException("privateKeyPath not set");
            }

            try (InputStream in = new FileInputStream(privateKeyPath)) {
                char[] pass = null;
                if (passphrasePath != null) {
                    pass = new String(Files.readAllBytes(passphrasePath))
                        .toCharArray();
                }

                RSAPrivateKey privateKey =
                    new PrivateKeyProvider(in, pass).getKey();

                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                RSAPrivateCrtKeySpec keySpec = keyFactory.getKeySpec(
                    keyFactory.translateKey(privateKey),
                    RSAPrivateCrtKeySpec.class);
                RSAPublicKey publicKey =
                    (RSAPublicKey) keyFactory.generatePublic(
                        new RSAPublicKeySpec(keySpec.getModulus(),
                                             keySpec.getPublicExponent()));
                keyPair = new KeyPair(publicKey, privateKey);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(
                    privateKeyPath + " doesn't exist", e);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "Unable to read passphrase from " + passphrasePath, e);
            } catch (NoSuchAlgorithmException |
                     InvalidKeyException |
                     InvalidKeySpecException e) {
                throw new IllegalStateException(
                    "Unable to intercept private key", e);
            }
        }
    }

    /**
     * @hidden
     * This is a helper class to get key pair by given fixed values of private
     * key and optional passphrase. The key pair is always returned.
     */
    static class FixedKeyPairSupplier implements SessionKeyPairSupplier {
        final private KeyPair keyPair;

        FixedKeyPairSupplier(String privateKeyContents, char[] passphrase) {
            RSAPrivateKey privateKey = new PrivateKeyProvider(
                new ByteArrayInputStream(privateKeyContents.getBytes()),
                                         passphrase).getKey();

            try {
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                RSAPrivateCrtKeySpec keySpec = keyFactory.getKeySpec(
                    keyFactory.translateKey(privateKey),
                    RSAPrivateCrtKeySpec.class);
                RSAPublicKey publicKey = (RSAPublicKey)
                    keyFactory.generatePublic(new RSAPublicKeySpec(
                        keySpec.getModulus(), keySpec.getPublicExponent()));
                keyPair = new KeyPair(publicKey, privateKey);
            } catch (NoSuchAlgorithmException |
                     InvalidKeyException |
                     InvalidKeySpecException e) {
                throw new IllegalStateException(
                    "Unable to intercept private key " + privateKeyContents,
                    e);
            }
        }

        @Override
        public KeyPair getKeyPair() {
            return keyPair;
        }

        @Override
        public void refreshKeys() {
            /* fixed key no need to refresh */
        }
    }
}
