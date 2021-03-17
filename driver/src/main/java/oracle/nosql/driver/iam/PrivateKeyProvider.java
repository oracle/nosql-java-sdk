/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.security.Security;
import java.security.interfaces.RSAPrivateKey;
import java.util.Arrays;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.EncryptionException;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

/**
 * @hidden
 * Internal use only
 * <p>
 * The RSA private key provider that loads and caches private key from input
 * stream using bouncy castle PEM key utilities.
 */
class PrivateKeyProvider {
    private final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
    private RSAPrivateKey key = null;

    /**
     * Build private key based on {@link AuthenticationProfileProvider}.
     */
    PrivateKeyProvider(AuthenticationProfileProvider provider) {
        this(provider.getPrivateKey(), provider.getPassphraseCharacters());
    }

    /**
     * Build private key provider from given input stream.
     */
    PrivateKeyProvider(InputStream keyInputStream, char[] passphrase) {
        getKeyInternal(keyInputStream, passphrase);
    }

    /**
     * Get the RSAPrivateKey
     */
    RSAPrivateKey getKey() {
        return key;
    }

    void reload(InputStream keyInputStream, char[] passphrase) {
        getKeyInternal(keyInputStream, passphrase);
    }

    void getKeyInternal(InputStream keyInputStream, char[] passphrase) {
        PEMParser keyReader = null;
        try {
            keyReader = new PEMParser(
                new InputStreamReader(keyInputStream, StandardCharsets.UTF_8));

            Object object = null;
            try {
                object = keyReader.readObject();
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Error reading private key", ioe);
            }
            PrivateKeyInfo keyInfo;

            if (object instanceof PEMEncryptedKeyPair) {
                requireNonNullIAE(
                    passphrase,
                    "The provided private key requires a passphrase");

                JcePEMDecryptorProviderBuilder decryptBuilder =
                    new JcePEMDecryptorProviderBuilder();

                if (!isProviderInstalled()) {
                    /*
                     * If BouncyCastle is not installed, must add the provider
                     * explicitly to enable the PEMDecrptorProvider.
                     * https://github.com/bcgit/bc-java/issues/156
                     */
                    decryptBuilder.setProvider(getBouncyCastleProvider());
                }

                PEMDecryptorProvider decProv = decryptBuilder.build(passphrase);
                try {
                    keyInfo = ((PEMEncryptedKeyPair) object)
                        .decryptKeyPair(decProv)
                        .getPrivateKeyInfo();
                } catch (EncryptionException ee) {
                    throw new IllegalArgumentException(
                            "The provided passphrase is incorrect.", ee);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException(
                        "Error decrypting private key.", ioe);
                }
            } else if (object instanceof PrivateKeyInfo) {
                keyInfo = (PrivateKeyInfo) object;
            } else if (object instanceof PEMKeyPair) {
                keyInfo = ((PEMKeyPair) object).getPrivateKeyInfo();
            } else if (object instanceof SubjectPublicKeyInfo) {
                throw new IllegalArgumentException(
                    "Public key provided instead of private key");
            } else if (object != null) {
                throw new IllegalArgumentException(
                    "Private key must be in PEM format," +
                    "was: " + object.getClass());
            } else {
                throw new IllegalArgumentException(
                    "Private key must be in PEM format");
            }

            try {
                this.key = (RSAPrivateKey) converter.getPrivateKey(keyInfo);
            } catch (PEMException e) {
                throw new IllegalArgumentException(
                    "Error converting private key");
            }
        } finally {
            if (keyReader != null) {
                try {
                    keyReader.close();
                } catch (IOException e) {
                    /* ignore */
                }
            }
            if (keyInputStream != null) {
                try {
                    keyInputStream.close();
                } catch (IOException e) {
                    /* ignore */
                }
            }
            if (passphrase != null) {
                Arrays.fill(passphrase, ' ');
            }
        }
    }

    private static boolean isProviderInstalled() {
        return (Security.getProvider("BC") != null);
    }

    private static Provider getBouncyCastleProvider() {
        Provider provider = null;
        try {
            Class<?> providerClass = Class.forName(
                "org.bouncycastle.jce.provider.BouncyCastleProvider");
            provider = (Provider)providerClass
                .getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                "Unable to find bouncy castle provider");
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Error creating bouncy castle provider");
        }
        return provider;
    }
}
