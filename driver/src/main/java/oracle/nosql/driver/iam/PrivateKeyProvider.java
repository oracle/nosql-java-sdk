/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Arrays;

import oracle.nosql.driver.iam.pki.Pem;
import oracle.nosql.driver.iam.pki.PemEncryptionException;
import oracle.nosql.driver.iam.pki.PemException;

/**
 * @hidden
 * Internal use only
 * <p>
 * The RSA private key provider that loads and caches private key from input
 * stream using PEM key utilities in oci-java-sdk.
 *
 * See com.oracle.bmc.http.signing.internal.PEMStreamRSAPrivateKeySupplier
 * for reference.
 */
class PrivateKeyProvider {
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
        try (ReadableByteChannel channel = Channels.newChannel(keyInputStream);
             Pem.Passphrase pemPassphrase = Pem.Passphrase.of(passphrase)){
            PrivateKey privateKey =
                Pem.decoder().with(pemPassphrase).decodePrivateKey(channel);
            if (privateKey instanceof RSAPrivateKey) {
                key = (RSAPrivateKey) privateKey;
            } else {
                throw new IllegalArgumentException(
                    "Must be RSA private key, but " + privateKey.toString());
            }
        } catch (PemEncryptionException e) {
            throw new IllegalArgumentException(
                "The provided passphrase is incorrect.", e);
        } catch (PemException e) {
            throw new IllegalArgumentException(
                "Private key must be in PEM format", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error reading private key", e);
        } finally {
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
}
