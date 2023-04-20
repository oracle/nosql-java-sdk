/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
import java.text.MessageFormat;
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

    /* Map used to lookup IAM URI */
    private static final Map<String, String> IAM_URI = new HashMap<>();
    private final static MessageFormat OC1_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud.com");
    private final static MessageFormat GOV_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclegovcloud.com");
    private final static MessageFormat OC4_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclegovcloud.uk");
    private final static MessageFormat OC5_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud5.com");
    private final static MessageFormat OC8_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud8.com");
    private final static MessageFormat OC9_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud9.com");
    private final static MessageFormat OC10_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud10.com");
    private final static MessageFormat OC14_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud14.com");
    private final static MessageFormat OC16_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud16.com");
    private final static MessageFormat OC17_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud17.com");
    private final static MessageFormat OC19_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud.eu");
    private final static MessageFormat OC20_EP_BASE = new MessageFormat(
        "https://auth.{0}.oraclecloud20.com");

    static {
        /* OC1 */
        IAM_URI.put("jnb", OC1_EP_BASE.format(new Object[] {"af-johannesburg-1"}));

        IAM_URI.put("yny", OC1_EP_BASE.format(new Object[] {"ap-chuncheon-1"}));
        IAM_URI.put("hyd", OC1_EP_BASE.format(new Object[] {"ap-hyderabad-1"}));
        IAM_URI.put("mel", OC1_EP_BASE.format(new Object[] {"ap-melbourne-1"}));
        IAM_URI.put("bom", OC1_EP_BASE.format(new Object[] {"ap-mumbai-1"}));
        IAM_URI.put("kix", OC1_EP_BASE.format(new Object[] {"ap-osaka-1"}));
        IAM_URI.put("icn", OC1_EP_BASE.format(new Object[] {"ap-seoul-1"}));
        IAM_URI.put("sin", OC1_EP_BASE.format(new Object[] {"ap-singapore-1"}));
        IAM_URI.put("syd", OC1_EP_BASE.format(new Object[] {"ap-sydney-1"}));
        IAM_URI.put("nrt", OC1_EP_BASE.format(new Object[] {"ap-tokyo-1"}));

        IAM_URI.put("cwl", OC1_EP_BASE.format(new Object[] {"uk-cardiff-1"}));
        IAM_URI.put("lhr", OC1_EP_BASE.format(new Object[] {"uk-london-1"}));

        IAM_URI.put("ams", OC1_EP_BASE.format(new Object[] {"eu-amsterdam-1"}));
        IAM_URI.put("fra", OC1_EP_BASE.format(new Object[] {"eu-frankfurt-1"}));
        IAM_URI.put("mad", OC1_EP_BASE.format(new Object[] {"eu-madrid-1"}));
        IAM_URI.put("mrs", OC1_EP_BASE.format(new Object[] {"eu-marseille-1"}));
        IAM_URI.put("lin", OC1_EP_BASE.format(new Object[] {"eu-milan-1"}));
        IAM_URI.put("cdg", OC1_EP_BASE.format(new Object[] {"eu-paris-1"}));
        IAM_URI.put("arn", OC1_EP_BASE.format(new Object[] {"eu-stockholm-1"}));
        IAM_URI.put("zrh", OC1_EP_BASE.format(new Object[] {"eu-zurich-1"}));

        IAM_URI.put("auh", OC1_EP_BASE.format(new Object[] {"me-abudhabi-1"}));
        IAM_URI.put("dxb", OC1_EP_BASE.format(new Object[] {"me-dubai-1"}));
        IAM_URI.put("jed", OC1_EP_BASE.format(new Object[] {"me-jeddah-1"}));

        IAM_URI.put("qro", OC1_EP_BASE.format(new Object[] {"mx-queretaro-1"}));
        IAM_URI.put("mty", OC1_EP_BASE.format(new Object[] {"mx-monterrey-1"}));

        IAM_URI.put("mtz", OC1_EP_BASE.format(new Object[] {"il-jerusalem-1"}));

        IAM_URI.put("gru", OC1_EP_BASE.format(new Object[] {"sa-saopaulo-1"}));
        IAM_URI.put("scl", OC1_EP_BASE.format(new Object[] {"sa-santiago-1"}));
        IAM_URI.put("vcp", OC1_EP_BASE.format(new Object[] {"sa-vinhedo-1"}));

        IAM_URI.put("phx", OC1_EP_BASE.format(new Object[] {"us-phoenix-1"}));
        IAM_URI.put("iad", OC1_EP_BASE.format(new Object[] {"us-ashburn-1"}));
        IAM_URI.put("sjc", OC1_EP_BASE.format(new Object[] {"us-sanjose-1"}));
        IAM_URI.put("ord", OC1_EP_BASE.format(new Object[] {"us-chicago-1"}));

        IAM_URI.put("yyz", OC1_EP_BASE.format(new Object[] {"ca-toronto-1"}));
        IAM_URI.put("yul", OC1_EP_BASE.format(new Object[] {"ca-montreal-1"}));

        /* OC2 */
        IAM_URI.put("lfi", GOV_EP_BASE.format(new Object[] {"us-langley-1"}));
        IAM_URI.put("luf", GOV_EP_BASE.format(new Object[] {"us-luke-1"}));

        /* OC3 */
        IAM_URI.put("ric", GOV_EP_BASE.format(new Object[] {"us-gov-ashburn-1"}));
        IAM_URI.put("pia", GOV_EP_BASE.format(new Object[] {"us-gov-chicago-1"}));
        IAM_URI.put("tus", GOV_EP_BASE.format(new Object[] {"us-gov-phoenix-1"}));

        /* OC4 */
        IAM_URI.put("ltn", OC4_EP_BASE.format(new Object[] {"uk-gov-london-1"}));
        IAM_URI.put("brs", OC4_EP_BASE.format(new Object[] {"uk-gov-cardiff-1"}));

        /* OC5 */
        IAM_URI.put("tiw", OC5_EP_BASE.format(new Object[] {"us-tacoma-1"}));

        /* OC8 */
        IAM_URI.put("nja", OC8_EP_BASE.format(new Object[] {"ap-chiyoda-1"}));
        IAM_URI.put("ukb", OC8_EP_BASE.format(new Object[] {"ap-ibaraki-1"}));

        /* OC9 */
        IAM_URI.put("mct", OC9_EP_BASE.format(new Object[] {"me-dcc-muscat-1"}));

        /* OC10 */
        IAM_URI.put("wga", OC10_EP_BASE.format(new Object[] {"ap-dcc-canberra-1"}));

        /* OC14 */
        IAM_URI.put("ork", OC14_EP_BASE.format(new Object[] {"eu-dcc-dublin-1"}));
        IAM_URI.put("snn", OC14_EP_BASE.format(new Object[] {"eu-dcc-dublin-2"}));
        IAM_URI.put("bgy", OC14_EP_BASE.format(new Object[] {"eu-dcc-milan-1"}));
        IAM_URI.put("mxp", OC14_EP_BASE.format(new Object[] {"eu-dcc-milan-2"}));
        IAM_URI.put("dus", OC14_EP_BASE.format(new Object[] {"eu-dcc-rating-1"}));
        IAM_URI.put("dtm", OC14_EP_BASE.format(new Object[] {"eu-dcc-rating-2"}));

        /* OC16 */
        IAM_URI.put("sgu", OC16_EP_BASE.format(new Object[] {"us-westjordan-1"}));

        /* OC17 */
        IAM_URI.put("ifp", OC17_EP_BASE.format(new Object[] {"us-dcc-phoenix-1"}));
        IAM_URI.put("gcn", OC17_EP_BASE.format(new Object[] {"us-dcc-phoenix-2"}));
        IAM_URI.put("yum", OC17_EP_BASE.format(new Object[] {"us-dcc-phoenix-4"}));

        /* OC19 */
        IAM_URI.put("str", OC19_EP_BASE.format(new Object[] {"eu-frankfurt-2"}));
        IAM_URI.put("vll", OC19_EP_BASE.format(new Object[] {"eu-madrid-2"}));

        /* OC20 */
        IAM_URI.put("beg", OC20_EP_BASE.format(new Object[] {"eu-jovanovac-1"}));
    }

    static String getIAMURL(String regionIdOrCode) {
        String uri = IAM_URI.get(regionIdOrCode);
        if (uri == null) {
            if (Region.isOC1Region(regionIdOrCode)) {
                return OC1_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isGovRegion(regionIdOrCode)) {
                return GOV_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC4Region(regionIdOrCode)) {
                return OC4_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC5Region(regionIdOrCode)) {
                return OC5_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC8Region(regionIdOrCode)) {
                return OC8_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC9Region(regionIdOrCode)) {
                return OC9_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC10Region(regionIdOrCode)) {
                return OC10_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC14Region(regionIdOrCode)) {
                return OC14_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC16Region(regionIdOrCode)) {
                return OC16_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC17Region(regionIdOrCode)) {
                return OC17_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC19Region(regionIdOrCode)) {
                return OC19_EP_BASE.format(new Object[] {regionIdOrCode});
            }
            if (Region.isOC20Region(regionIdOrCode)) {
                return OC20_EP_BASE.format(new Object[] {regionIdOrCode});
            }
        }

        return uri;
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
