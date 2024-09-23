/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.util.CheckNull.requireNonNullIAE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.UUID;

import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.CertificateSupplier.X509CertificateKeyPair;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.AttributeTypeAndValue;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

/**
 * @hidden
 * Internal use only
 */
class Utils {
    private static final JsonFactory factory = new JsonFactory();
    /* Signing algorithm only rsa-sha256 is allowed */
    static final String RSA = "rsa-sha256";
    static final String RSA_JVM_NAME = "SHA256withRSA";

    /* OCI signature version only version 1 is allowed*/
    static final int SINGATURE_VERSION = 1;

    /* Constants used to build signature */
    static final String HEADER_DELIMITER = ": ";
    static String SIGNATURE_HEADER_FORMAT =
        "Signature headers=\"%s\",keyId=\"%s\",algorithm=\"%s\"," +
        "signature=\"%s\",version=\"%s\"";
    static final String DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

    /*
     * <ocid>.<resource-type>.<realm>. <region>(.future-extensibility)
     * .<resource-type-specific-id>
     * pattern is relaxed other than the required <ocid> and
     * <resource-type-specific-id>
     */
    private static final Pattern OCID_PATTERN = Pattern.compile(
        "^([0-9a-zA-Z-_]+[.:])([0-9a-zA-Z-_]*[.:]){3,}([0-9a-zA-Z-_]+)$");

    /* HEX chars used to compute certificate fingerprint */
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /* 4k bytes */
    private static final int BUF_SIZE = 0x800;

    /* fields in JWT token JSON used to check validity */
    private static final String[] FIELDS = {
        "exp", "jwk", "n", "e",
        SignatureProvider.ResourcePrincipalClaimKeys.COMPARTMENT_ID_CLAIM_KEY,
        SignatureProvider.ResourcePrincipalClaimKeys.TENANT_ID_CLAIM_KEY};

    static String getIAMURL(String regionIdOrCode) {
        Region region = Region.fromRegionIdOrCode(regionIdOrCode);
        if (region != null) {
            return region.endpointForService("auth",
                                             Region.getAuthEndpointFormat());
        }
        return null;
    }

    /**
     * Test if the given OCID matches the expected pattern for OCIDs.
     *
     * @param ocid The string to test.
     * @return true if it matches teh pattern, false if not.
     */
    static boolean isValidOcid(String ocid) {
        return OCID_PATTERN.matcher(ocid).matches();
    }

    /**
     * Creates a keyId from the individual components.
     * @param tenantId
     * @param userId
     * @param fingerprint
     * @return The keyId used to sign requests
     */
    static String createKeyId(String tenantId,
                              String userId,
                              String fingerprint) {
        return String.format("%s/%s/%s", tenantId, userId, fingerprint);
    }

    /**
     * Creates a keyId from an {@link AuthenticationDetailsProvider}.
     *
     * @param provider
     * @return The keyId used to sign requests
     */
    static String createKeyId(UserAuthenticationProfileProvider prov) {
        return createKeyId(prov.getTenantId(),
                           prov.getUserId(),
                           prov.getFingerprint());
    }

    /**
     * Attempts to expand paths that may contain unix-style home shorthand.
     */
    static String expandUserHome(final String path) {
        /* If the home (~) shortcut is used, then attempt to determine correct
         * path. Otherwise, leave as is to allow users to always be able to
         * specify a path without modifying it.
         */
        if (path.startsWith("~/") || path.startsWith("~\\")) {
            return System.getProperty("user.home") +
                   correctPath(isWindows(), path.substring(1));
        }
        return path;
    }

    private static boolean isWindows() {
        String os = System.getProperty("os.name");
        return (os.indexOf("Windows") != -1);
    }

    /*
     * Handle the case where somebody is copying the config file
     * between platforms (or copying examples without changing values)
     */
    private static String correctPath(boolean isWindows, String path) {
        if (isWindows) {
            /* https://msdn.microsoft.com/en-us/library/aa365247
             * forward slash is reserved, assume its not supposed to
             * be there and replace with back slash
             */
            path = path.replace('/', '\\');
        }
        /*
         * back slash is not a reserved character on other platforms,
         * so do not attempt to modify it
         */
        return path;
    }

    static SimpleDateFormat createFormatter() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT,
                                                           Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat;
    }

    static String sign(String signingContent, PrivateKey key)
        throws Exception {

        Signature signature = Signature.getInstance(RSA_JVM_NAME);
        signature.initSign(key);
        signature.update(signingContent.getBytes(StandardCharsets.UTF_8));
        byte[] bytes = signature.sign();
        return new String(Base64.getEncoder().encode(bytes),
                          StandardCharsets.UTF_8);
    }

    /**
     * Converts a private key back to a PEM formatted input stream.
     * @param key The key to convert.
     * @return A new input stream
     */
    static byte[] toByteArray(RSAPrivateKey key) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JcaPEMWriter writer = new JcaPEMWriter(
                 new OutputStreamWriter(baos, StandardCharsets.UTF_8))) {

            writer.writeObject(key);
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write PEM object", e);
        }

        return baos.toByteArray();
    }

    /**
     * Base64 encodes a public key with no chunking.
     * @param publicKey The public key
     * @return Base64 representation
     */
    static String base64EncodeNoChunking(RSAPublicKey publicKey) {
        return new String(
            Base64.getMimeEncoder().encode(publicKey.getEncoded()),
            StandardCharsets.UTF_8);
    }

   /**
    * Base64 encodes a X509Certificate with no chunking.
    * @param certificate The certificate
    * @return Base64 representation
    */
    static String base64EncodeNoChunking(X509CertificateKeyPair keyPair) {

        return new String(
            Base64.getMimeEncoder().encode(
                getEncodedCertificate(keyPair.getRawCertificate())),
                StandardCharsets.UTF_8);
    }

    /**
     * Gets the fingerprint of a certificate using SHA-256.
     * @param certificate the certificate
     * @return Fingerprint of the certificate
     * @throws Error if there is an error
     */
    static String getFingerPrint(X509CertificateKeyPair keyPair) {
        requireNonNullIAE(keyPair.getRawCertificate(),
                          "Unable to get certificate finger print, " +
                          "raw certificate is null");

        try {
            String pemCert = keyPair.getRawCertificate();
            byte[] encodedCertificate = getEncodedCertificate(pemCert);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(encodedCertificate);
            String fingerprint = getHex(md.digest());

            return formatFingerPrint(fingerprint);
        } catch (NoSuchAlgorithmException  e) {
            throw new IllegalStateException(
                "Unknown error getting certificate fingerprint", e);
        }
    }

    private static byte[] getEncodedCertificate(String pemCertificate) {
        /* strip out header and footer */
        return Base64.getMimeDecoder().decode(pemCertificate
                   .replace("-----BEGIN CERTIFICATE-----", "")
                   .replace("-----END CERTIFICATE-----", ""));
    }

    private static String formatFingerPrint(String fingerprint) {
        int length = fingerprint.length();
        char[] format = new char[length * 3 / 2 - 1];

        int j = 0;
        for (int i = 0; i < length - 2; i += 2) {
            format[j++] = fingerprint.charAt(i);
            format[j++] = fingerprint.charAt(i + 1);
            format[j++] = ':';
        }
        format[j++] = fingerprint.charAt(length - 2);
        format[j] = fingerprint.charAt(length - 1);

        return String.valueOf(format);
    }

    /**
     * Computes the hex representation of a byte array.
     */
    private static String getHex(byte bytes[]) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }

        return new String(hexChars);
    }

    /**
     * Get the tenant id from the given X509 certificate.
     * @param certificate the given certificate.
     * @return the tenant id.
     */
    static String getTenantId(X509Certificate certificate) {
        requireNonNullIAE(certificate,
                          "Unable to get tenant id, certificate is null");

        X500Name name = new X500Name(
            certificate.getSubjectX500Principal().getName());

        String tenantId = getValue(name, BCStyle.OU, "opc-tenant");
        if (tenantId == null) {
            tenantId = getValue(name, BCStyle.O, "opc-identity");
        }

        if (tenantId != null) {
            return tenantId;
        }
        throw new IllegalStateException(
            "The certificate used by instance principal " +
            "does not contain tenant id.");
    }

    private static String getValue(X500Name name,
                                   ASN1ObjectIdentifier id,
                                   String key) {
        String prefix = key + ":";
        for (RDN rdn : name.getRDNs(id)) {
            for (AttributeTypeAndValue typeAndValue : rdn.getTypesAndValues()) {
                String value = typeAndValue.getValue().toString();

                if (value.startsWith(prefix)) {
                    return value.substring(prefix.length());
                }
            }
        }
        return null;
    }

    static String readStream(InputStream inputStream)
        throws IOException {

        InputStreamReader reader = new InputStreamReader(inputStream,
                                                         StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[BUF_SIZE];

        int read;
        while ((read = reader.read(buf)) != -1) {
            sb.append(buf, 0, read);
        }
        return sb.toString();
    }

    /**
     * Convert modulus and exponent from JWK to a RSAPublicKey.
     */
    static RSAPublicKey toPublicKey(String modulus, String exponent) {
        try {
            /* modulus and exponent are unsigned, negative big integer should
             * be converted to positive
             */
            Decoder decoder = Base64.getUrlDecoder();
            RSAPublicKey key = (RSAPublicKey) KeyFactory.getInstance("RSA")
                .generatePublic(new RSAPublicKeySpec(
                new BigInteger(1, decoder.decode(modulus.getBytes())),
                new BigInteger(1, decoder.decode(exponent.getBytes()))));
            return key;
        } catch (Exception ex) {
            throw new IllegalStateException(
                "Error build public key from JWK", ex);
        }
    }

    static String computeBodySHA256(byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(body);
            byte[] hash = digest.digest();
            return new String(Base64.getEncoder().encodeToString(hash));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algorithm SHA-256 unavailable", e);
        }
    }

    static JsonParser createParser(String json)
        throws IOException {

        return factory.createParser(json);
    }

    static JsonGenerator createGenerator(StringWriter sw)
        throws IOException {

        return factory.createGenerator(sw);
    }

    static String findField(String json, JsonParser parser, String... name)
        throws IOException {

        List<String> fieldsList = Arrays.asList(name);
        JsonToken token;
        while ((token = parser.nextToken()) != null) {
            if (token == JsonToken.START_OBJECT ||
                token == JsonToken.END_OBJECT ||
                token == JsonToken.VALUE_STRING) {
                continue;
            }

            String fieldName = parser.getCurrentName();
            if (fieldName == null) {
                throw new IllegalStateException(
                    "Null token or field name found in JSON " + json);
            }
            if (token == JsonToken.FIELD_NAME) {
                if (fieldsList.stream().anyMatch(
                    str -> str.trim().equals(fieldName))) {
                    return fieldName;
                }
                token = parser.nextToken();
                if (token == JsonToken.START_ARRAY) {
                    while (true) {
                        if (parser.nextToken() == JsonToken.END_ARRAY) {
                            break;
                        }
                    }
                    continue;
                }
            }
        }
        return null;
    }

    /*
     * Parse JSON response that only has one field token.
     * { "token": "...."}
     */
    static String parseTokenResponse(String response) {
        try {
            JsonParser parser = createParser(response);
            if (parser.getCurrentToken() == null) {
                parser.nextToken();
            }
            while (parser.getCurrentToken() != null) {
                String field = findField(response, parser, "token");
                if (field != null) {
                    parser.nextToken();
                    return parser.getText();
                }
            }
            throw new IllegalStateException(
                "Unable to find security token in " + response);
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "Error parsing security token " + response +
                    " " + ioe.getMessage());
        }
    }

    /*
     * Parse security token JSON response get fields expiration time,
     * modulus and public exponent of JWK, only used for security token
     * validity check, ignores other fields.
     *
     * Response:
     * {
     *  "exp" : 1234123,
     *  "jwk" : {
     *    "e": "xxxx",
     *    "n": "xxxx",
     *    ...
     *  }
     *  ...
     * }
     */
    static Map<String, String> parseToken(String token) {
        if (token == null) {
            return null;
        }
        String[] jwt = splitJWT(token);
        String claimJson = new String(Base64.getUrlDecoder().decode(jwt[1]),
            StandardCharsets.UTF_8);

        try {
            JsonParser parser = createParser(claimJson);
            Map<String, String> results = new HashMap<>();
            parse(token, parser, results);

            String jwkString = results.get("jwk");
            if (jwkString == null) {
                return results;
            }
            parser = createParser(jwkString);
            parse(token, parser, results);

            return results;
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "Error parsing security token "+ ioe.getMessage());
        }
    }

    private static void parse(String json,
                              JsonParser parser,
                              Map<String, String> reults)
        throws IOException {

        if (parser.getCurrentToken() == null) {
            parser.nextToken();
        }
        while (parser.getCurrentToken() != null) {
            String field = findField(json, parser, FIELDS);
            if (field != null) {
                parser.nextToken();
                reults.put(field, parser.getText());
            }
        }
    }

    private static String[] splitJWT(String jwt) {
        final int dot1 = jwt.indexOf(".");
        final int dot2 = jwt.indexOf(".", dot1 + 1);
        final int dot3 = jwt.indexOf(".", dot2 + 1);

        if (dot1 == -1 || dot2 == -1 || dot3 != -1) {
            throw new IllegalArgumentException(
                "Given string is not in the valid access token format");
        }

        final String[] parts = new String[3];
        parts[0] = jwt.substring(0, dot1);
        parts[1] = jwt.substring(dot1 + 1, dot2);
        parts[2] = jwt.substring(dot2 + 1);
        return parts;
    }

    static String generateOpcRequestId() {
        return UUID.randomUUID().toString().replace("-", "").toUpperCase();
    }

    static void logTrace(Logger logger, String message) {
        if (logger != null) {
            logger.log(Level.FINE, message);
        }
    }

    static void logError(Logger logger, String message) {
        if (logger != null) {
            logger.log(Level.WARNING, message);
        }
    }
}
