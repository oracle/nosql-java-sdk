/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.HEADER_DELIMITER;
import static oracle.nosql.driver.iam.Utils.RSA;
import static oracle.nosql.driver.iam.Utils.SIGNATURE_HEADER_FORMAT;
import static oracle.nosql.driver.iam.Utils.SINGATURE_VERSION;
import static oracle.nosql.driver.iam.Utils.createFormatter;
import static oracle.nosql.driver.iam.Utils.sign;
import static oracle.nosql.driver.util.HttpConstants.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.Region.RegionProvider;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityTokenBasedProvider;
import oracle.nosql.driver.ops.Request;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * Cloud service only.
 * <p>
 * An instance of {@link AuthorizationProvider} that generates and caches
 * signature for each request as authorization string. A number of pieces of
 * information are required for configuration. See
 * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
 * and
 * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm">Required Keys and OCIDs</a>
 * for additional information as well as instructions on how to create required
 * keys and OCIDs for configuration. The required information includes:
 * <ul>
 * <li>A signing key, used to sign requests.</li>
 * <li>A pass phrase for the key, if it is encrypted</li>
 * <li>The fingerprint of the key pair used for signing</li>
 * <li>The OCID of the tenancy</li>
 * <li>The OCID of a user in the tenancy</li>
 * </ul>
 * All of this information is required to authenticate and authorize access to
 * the service.
 * <p>
 * There are three mechanisms for providing authorization information:
 * <ol>
 * <li>Using a user's identity and optional profile. This authenticates and
 * authorizes the application based on a specific user identity.</li>
 * <li>Using an Instance Principal, which can be done when running on a
 * compute instance in the Oracle Cloud Infrastructure (OCI). See
 * {@link #createWithInstancePrincipal} and
 * <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
 * </li>
 * <li>Using a Resource Principal, which is usually done when running
 * in an OCI Function. See {@link #createWithResourcePrincipal} and
 * <a href="https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm">Accessing Other Oracle Cloud Infrastructure Resources from Running Functions</a>
 * </li>
 * </ol>
 * <p>
 * When using the first one, a User Principal, a default compartment is
 * used and that is the root compartment of the user's tenancy. If a specific
 * compartment is used (recommended) it can be specified as a default
 * or per-request. In addition when using a User Principal compartments can
 * be named by compartment name vs OCID when naming compartments and tables
 * in {@link Request} classes and when naming tables in queries.
 * <p>
 * When using an Instance Principal or Resource Principal a compartment
 * must be specified as there is no default for these principal types. In
 * addition these principal types limit the ability to use a compartment
 * name vs OCID when naming compartments and tables in {@link Request}
 * classes and when naming tables
 * in queries.
 * <p>
 * When using a specific user's identity there are several options to provide
 * the required information:
 * <ul>
 * <li>Using a configuration file. See
 * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
 * for details on the file contents. By default the file is stored in
 * ~/.oci/config, but you may supply a path to another location. The
 * configuration file may include multiple profiles. The constructors that use
 * a configuration include
 * <ul>
 * <li>{@link #SignatureProvider()}</li>
 * <li>{@link #SignatureProvider(String)}</li>
 * <li>{@link #SignatureProvider(String, String)}</li>
 * </ul>
 * </li>
 * <li>Using information passed programmatically. The constructors that use this
 * mechanism include
 * <ul>
 * <li>{@link #SignatureProvider(String, String, String, String, char[])}</li>
 * <li>{@link #SignatureProvider(String, String, String, File, char[])}</li>
 * </ul>
 * </li>
 * </ul>
 */
public class SignatureProvider
    implements AuthorizationProvider, RegionProvider {

    private static final String SIGNING_HEADERS = "(request-target) host date";
    private static final String SIGNING_HEADERS_WITH_OBO =
        "(request-target) host date opc-obo-token";
    private static final String OBO_TOKEN_HEADER = "opc-obo-token";

    /* Maximum lifetime of signature 240 seconds */
    protected static final int MAX_ENTRY_LIFE_TIME = 240;

    /* Default refresh time before signature expiry, 20 seconds*/
    protected static final int DEFAULT_REFRESH_AHEAD = 20000;

    /* User profile and key providers */
    private final AuthenticationProfileProvider provider;
    private final PrivateKeyProvider privateKeyProvider;

    /* Delegation token specified for signing */
    private String delegationToken;

    /* the currently cached signature */
    private SignatureDetails currentSigDetails;

    /* new signature, in process of warmup */
    private SignatureDetails refreshSigDetails;

    /* Refresh timer */
    private volatile Timer refresher;

    /* Refresh time before signature expires */
    private long refreshAheadMs = DEFAULT_REFRESH_AHEAD;

    /* Refresh interval, if zero, no refresh will be scheduled */
    private long refreshIntervalMs = 0;

    private String serviceHost;
    private Region region;
    private Logger logger;

    /**
     * @hidden
     * A callback interface called when the signature is refreshed. This
     * allows the application to perform an operation or 2 that can be used
     * to refresh the server-side authorization information out of its main
     * request path, reducing average latency
     */
    @FunctionalInterface
    public interface OnSignatureRefresh {
        /* refreshMs is the max amount of time this operation can/should take */
        public void refresh(long refreshMs);
    }

    private OnSignatureRefresh onSigRefresh;

    /**
     * Creates a SignatureProvider using a default configuration file and
     * profile. The configuration file used is <code>~/.oci/config</code>. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @throws IOException if error loading profile from OCI configuration file
     */
    public SignatureProvider() throws IOException {
        this(new OCIConfigFileProvider());
    }

    /**
     * Creates a SignatureProvider using the specified profile. The
     * configuration file used is <code>~/.oci/config</code>. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param profileName user profile name
     *
     * @throws IOException if error loading profile from OCI configuration file
     */
    public SignatureProvider(String profileName) throws IOException {
        this(new OCIConfigFileProvider(profileName));
    }

    /**
     * Creates a SignatureProvider using the specified config file and
     * profile. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param configFile path of configuration file
     *
     * @param profileName user profile name
     *
     * @throws IOException if error loading profile from OCI configuration file
     */
    public SignatureProvider(String configFile, String profileName)
        throws IOException {

        this(new OCIConfigFileProvider(configFile, profileName));
    }

    /**
     * Creates a SignatureProvider using directly provided user authentication
     * information. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm">Required Keys and OCIDs</a>
     * for details of the required parameters.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param tenantId tenant id
     *
     * @param userId user id
     *
     * @param fingerprint fingerprint of the key being used
     *
     * @param privateKey the string of private key used to sign request
     *
     * @param passphrase optional passphrase for the (encrypted) private key
     */
    public SignatureProvider(String tenantId,
                             String userId,
                             String fingerprint,
                             String privateKey,
                             char[] passphrase) {

        this(SimpleProfileProvider.builder()
             .tenantId(tenantId)
             .userId(userId)
             .fingerprint(fingerprint)
             .passphrase(passphrase)
             .privateKeySupplier(new PrivateKeyStringSupplier(privateKey))
             .build());
    }

    /**
     * Creates a SignatureProvider using directly provided user authentication
     * information. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm">Required Keys and OCIDs</a>
     * for details of the required parameters.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param tenantId tenant id
     *
     * @param userId user id
     *
     * @param fingerprint fingerprint of the key being used
     *
     * @param privateKeyFile the file of the private key used to sign request
     *
     * @param passphrase optional passphrase for the (encrypted) private key
     */
    public SignatureProvider(String tenantId,
                             String userId,
                             String fingerprint,
                             File privateKeyFile,
                             char[] passphrase) {

        this(SimpleProfileProvider.builder()
             .tenantId(tenantId)
             .userId(userId)
             .fingerprint(fingerprint)
             .passphrase(passphrase)
             .privateKeySupplier(new PrivateKeyFileSupplier(privateKeyFile))
             .build());
    }

    /**
     * Creates a SignatureProvider using directly provided user authentication
     * information. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm">Required Keys and OCIDs</a>
     * for details of the required parameters.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param tenantId tenant id
     *
     * @param userId user id
     *
     * @param fingerprint fingerprint of the key being used
     *
     * @param privateKeyFile the file of the private key used to sign request
     *
     * @param passphrase optional passphrase for the (encrypted) private key
     *
     * @param region identifies the region will be accessed by the NoSQLHandle.
     */
    public SignatureProvider(String tenantId,
                             String userId,
                             String fingerprint,
                             File privateKeyFile,
                             char[] passphrase,
                             Region region) {

        this(SimpleProfileProvider.builder()
             .tenantId(tenantId)
             .userId(userId)
             .fingerprint(fingerprint)
             .passphrase(passphrase)
             .privateKeySupplier(new PrivateKeyFileSupplier(privateKeyFile))
             .region(region)
             .build());
    }

    /**
     * Creates a SignatureProvider using an instance principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from an Oracle Cloud compute instance.
     * It authenticates with the instance principal and
     * uses a security token issued by IAM to do the actual request signing.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     * @return SignatureProvider
     */
    public static SignatureProvider createWithInstancePrincipal() {
        return new SignatureProvider(
            InstancePrincipalsProvider.builder().build());
    }

    /**
     * Creates a SignatureProvider using an instance principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from an Oracle Cloud compute instance.
     * It authenticates with the instance principal and
     * uses a security token issued by IAM to do the actual request signing.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param region identifies the region will be accessed by the NoSQLHandle.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithInstancePrincipal(Region region) {
        return new SignatureProvider(
            InstancePrincipalsProvider.builder().setRegion(region).build());
    }

    /**
     * Creates a SignatureProvider using an instance principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from an Oracle Cloud compute instance.
     * It authenticates with the instance principal and
     * uses a security token issued by IAM to do the actual request signing.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param iamAuthUrl The URL is usually detected automatically, specify
     * the URL if you need to overwrite the default, or the region of instance
     * doesn't exists in registered regions listed in {@link Region}.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipal(String iamAuthUrl) {

        return new SignatureProvider(
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(iamAuthUrl)
            .build());
    }

    /**
     * Creates a SignatureProvider using an instance principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from an Oracle Cloud compute instance.
     * It authenticates with the instance principal and
     * uses a security token issued by IAM to do the actual request signing.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param iamAuthUrl The URL is usually detected automatically, specify
     * the URL if you need to overwrite the default, or the region of instance
     * doesn't exists in registered regions listed in {@link Region}.
     * @param region the region to use, it may be null
     * @param logger the logger used by the SignatureProvider.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipal(String iamAuthUrl,
                                    Region region,
                                    Logger logger) {

        SignatureProvider provider = new SignatureProvider(
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(iamAuthUrl)
            .setLogger(logger)
            .setRegion(region)
            .build());
        provider.setLogger(logger);
        return provider;
    }

    /**
     * Creates a SignatureProvider using an instance principal with a
     * delegation token. This constructor may be used when calling the
     * Oracle NoSQL Database Cloud Service from an Oracle Cloud compute
     * instance. It authenticates with the instance principal and uses a
     * security token issued by IAM to do the actual request signing.
     * The delegation token allows the instance to assume the privileges
     * of the user for which the token was created.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param delegationToken the string of delegation token that allows an
     * instance to assume the privileges of a specific user and act
     * on-behalf-of that user
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipalForDelegation(String delegationToken) {

        SignatureProvider provider = new SignatureProvider(
            InstancePrincipalsProvider.builder().build());
        provider.setDelegationToken(delegationToken);
        return provider;
    }

    /**
     * Creates a SignatureProvider using an instance principal with a
     * delegation token. This constructor may be used when calling the
     * Oracle NoSQL Database Cloud Service from an Oracle Cloud compute
     * instance. It authenticates with the instance principal and uses a
     * security token issued by IAM to do the actual request signing.
     * The delegation token allows the instance to assume the privileges
     * of the user for which the token was created.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param iamAuthUrl The URL is usually detected automatically, specify
     * the URL if you need to overwrite the default, or the region of instance
     * doesn't exists in registered regions listed in {@link Region}.
     * @param region the region to use, it may be null
     * @param delegationToken the string of delegation token that allows an
     * instance to assume the privileges of a specific user and act
     * on-behalf-of that user
     * @param logger the logger used by the SignatureProvider.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipalForDelegation(String iamAuthUrl,
                                                 Region region,
                                                 String delegationToken,
                                                 Logger logger) {

        SignatureProvider provider = new SignatureProvider(
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(iamAuthUrl)
            .setLogger(logger)
            .setRegion(region)
            .build());
        provider.setLogger(logger);
        provider.setDelegationToken(delegationToken);
        return provider;
    }

    /**
     * Creates a SignatureProvider using an instance principal with a
     * delegation token. This constructor may be used when calling the
     * Oracle NoSQL Database Cloud Service from an Oracle Cloud compute
     * instance. It authenticates with the instance principal and uses a
     * security token issued by IAM to do the actual request signing.
     * The delegation token allows the instance to assume the privileges
     * of the user for which the token was created.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param delegationTokenFile the file of delegation token that allows
     * an instance to assume the privileges of a specific user and act
     * on-behalf-of that user. Note that the file must only contains full
     * string of the token.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipalForDelegation(File delegationTokenFile) {

        SignatureProvider provider = new SignatureProvider(
            InstancePrincipalsProvider.builder().build());
        String token;
        try {
            token = readFile(delegationTokenFile);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Unable to read the token from " + delegationTokenFile, e);
        }
        provider.setDelegationToken(token);
        return provider;
    }

    /**
     * Creates a SignatureProvider using an instance principal with a
     * delegation token. This constructor may be used when calling the
     * Oracle NoSQL Database Cloud Service from an Oracle Cloud compute
     * instance. It authenticates with the instance principal and uses a
     * security token issued by IAM to do the actual request signing.
     * The delegation token allows the instance to assume the privileges
     * of the user for which the token was created.
     * <p>
     * When using an instance principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>.
     *
     * @param iamAuthUrl The URL is usually detected automatically, specify
     * the URL if you need to overwrite the default, or the region of instance
     * doesn't exists in registered regions listed in {@link Region}.
     * @param region the region to use, it may be null
     * @param delegationTokenFile the file of delegation token that allows
     * an instance to assume the privileges of a specific user and act
     * on-behalf-of that user. Note that the file must only contains full
     * string of the token.
     * @param logger the logger used by the SignatureProvider.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithInstancePrincipalForDelegation(String iamAuthUrl,
                                                 Region region,
                                                 File delegationTokenFile,
                                                 Logger logger) {

        SignatureProvider provider = new SignatureProvider(
            InstancePrincipalsProvider.builder()
            .setFederationEndpoint(iamAuthUrl)
            .setLogger(logger)
            .setRegion(region)
            .build());
        provider.setLogger(logger);
        String token;
        try {
            token = readFile(delegationTokenFile);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Unable to read the token from " + delegationTokenFile, e);
        }
        provider.setDelegationToken(token);
        return provider;
    }

    /**
     * Creates a SignatureProvider using a resource principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from other Oracle Cloud service resource such as
     * Functions. It uses a resource provider session token (RPST) that
     * enables the resource such as function to authenticate itself.
     * <p>
     * When using an resource principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm">Accessing Other Oracle Cloud Infrastructure Resources from Running Functions</a>.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithResourcePrincipal() {
        return SignatureProvider.createWithResourcePrincipal(null);
    }

    /**
     * Creates a SignatureProvider using a resource principal. This
     * constructor may be used when calling the Oracle NoSQL Database
     * Cloud Service from other Oracle Cloud Service resource such as
     * Functions. It uses a resource provider session token (RPST) that
     * enables the resource such as the function to authenticate itself.
     *
     * <p>
     * When using an resource principal the compartment id (OCID) must be
     * specified on each request or defaulted by using
     * {@link NoSQLHandleConfig#setDefaultCompartment}. If the compartment id
     * is not specified for an operation an exception will be thrown.
     * <p>
     * See <a href="https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm">Accessing Other Oracle Cloud Infrastructure Resources from Running Functions</a>.
     *
     * @param logger the logger used by the SignatureProvider
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithResourcePrincipal(Logger logger) {
        SignatureProvider provider = new SignatureProvider(
            ResourcePrincipalProvider.build(logger));
        provider.setLogger(logger);
        return provider;
    }

    /*
     * The SignatureProvider that generates and caches request signature using
     * key id and private key supplied by {@link AuthenticationProfileProvider}.
     */
    protected SignatureProvider(AuthenticationProfileProvider provider) {
        this(provider, MAX_ENTRY_LIFE_TIME, DEFAULT_REFRESH_AHEAD);
    }

    /*
     * The constructor that is able to set refresh time before signature
     * expire, currently this is hidden for simplicity.
     */
    protected SignatureProvider(AuthenticationProfileProvider profileProvider,
                                int durationSeconds,
                                int refreshAhead) {
        if (profileProvider instanceof RegionProvider) {
            this.region = ((RegionProvider) profileProvider).getRegion();
        }
        this.provider = profileProvider;
        this.privateKeyProvider = (provider == null) ?
            null : new PrivateKeyProvider(provider);

        if (durationSeconds > MAX_ENTRY_LIFE_TIME) {
            throw new IllegalArgumentException(
                "Signature cannot be cached longer than " +
                MAX_ENTRY_LIFE_TIME + " seconds");
        }

        this.refreshAheadMs = refreshAhead;
        long durationMS = durationSeconds * 1000;
        if (durationMS > refreshAheadMs) {
            this.refreshIntervalMs = durationMS - refreshAheadMs;
        }
        if (this.provider instanceof SecurityTokenBasedProvider) {
            ((SecurityTokenBasedProvider) provider)
                .setMinTokenLifetime(durationMS);
        }
    }

    @Override
    public String getAuthorizationString(Request request) {
        if (serviceHost == null) {
            throw new IllegalArgumentException(
               "Unable to find service host, use setServiceHost " +
               "to load from NoSQLHandleConfig");
        }
        SignatureDetails sigDetails = getSignatureDetails(request);
        if (sigDetails != null) {
            return sigDetails.getSignatureHeader();
        }
        return null;
    }

    @Override
    public void setRequiredHeaders(String authString,
                                   Request request,
                                   HttpHeaders headers) {

        SignatureDetails sigDetails = getSignatureDetails(request);
        if (sigDetails == null) {
            return;
        }
        headers.add(AUTHORIZATION, sigDetails.getSignatureHeader());
        headers.add(DATE, sigDetails.getDate());

        if (delegationToken != null) {
            headers.add(OBO_TOKEN_HEADER, delegationToken);
        }
        String compartment = request.getCompartment();
        if (compartment == null) {
            /*
             * If request doesn't has compartment id, set the tenant id as the
             * default compartment, which is the root compartment in IAM if
             * using user principal. If using an instance principal this
             * value is null.
             */
            compartment = getTenantOCID();
        }

        if (compartment != null) {
            headers.add(REQUEST_COMPARTMENT_ID, compartment);
        } else {
            throw new IllegalArgumentException(
                "Compartment is null. When authenticating using an " +
                "Instance Principal the compartment for the operation " +
                "must be specified.");
        }
    }

    @Override
    public synchronized void flushCache() {
        currentSigDetails = null;
        refreshSigDetails = null;
    }

    /**
     * Get tenant OCID if using user principal.
     * @return tenant OCID of user
     */
    private String getTenantOCID() {
        if (provider instanceof UserAuthenticationProfileProvider) {
            return ((UserAuthenticationProfileProvider)this.provider)
                .getTenantId();
        }
        return null;
    }

    @Override
    public void close() {
        if (refresher != null) {
            refresher.cancel();
            refresher = null;
        }
        if (provider instanceof InstancePrincipalsProvider) {
            ((InstancePrincipalsProvider)this.provider).close();
        }
    }

    @Override
    public Region getRegion() {
        return region;
    }

    /**
     * @hidden
     * Internal use only.
     * <p>
     * Prepare SignatureProvider with given NoSQLHandleConfig. It configures
     * service URL, creates and caches the signature as warm-up. This
     * method should be called when the NoSQLHandle is created.
     * @param config the configuration
     * @return this
     */
    public SignatureProvider prepare(NoSQLHandleConfig config) {
        URL serviceURL = config.getServiceURL();
        if (serviceURL == null) {
            throw new IllegalArgumentException(
                "Must set service URL first");
        }
        this.serviceHost = serviceURL.getHost();

        /*
         * pass SSL related configuration to its SecurityTokenSupplier
         * to create the HTTP client
         */
        if (provider instanceof InstancePrincipalsProvider) {
            ((InstancePrincipalsProvider)this.provider).prepare(config);
        }

        /* creates and caches a signature as warm-up */
        getSignatureDetailsInternal(false);
        return this;
    }

    /**
     * Sets a Logger instance for this provider. If not set, the logger
     * associated with the driver is used.
     *
     * @param logger the logger
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Returns the logger of this provider if set, null if not.
     *
     * @return logger
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Resource principal session tokens carry JWT claims. Permit the
     * retrieval of the value from the token by given key.
     * See {@link ResourcePrincipalClaimKeys}
     *
     * @param key the name of a claim in the session token
     *
     * @return the claim value.
     */
    public String getResourcePrincipalClaim(String key) {
        if (!(provider instanceof ResourcePrincipalProvider)) {
            throw new IllegalArgumentException(
                "Only resource principal support");
        }
        ResourcePrincipalProvider rpp = (ResourcePrincipalProvider)provider;
        return rpp.getClaim(key);
    }

    /* visible for testing */
    void setDelegationToken(String token) {
        /* simple token format validation */
        int dot1 = token.indexOf(".");
        int dot2 = token.indexOf(".", dot1 + 1);
        int dot3 = token.indexOf(".", dot2 + 1);

        if (dot1 == -1 || dot2 == -1 || dot3 != -1) {
            throw new IllegalArgumentException(
                "Given string is not in the valid JWT token format\n" +
                token);
        }
        this.delegationToken = token;
    }

    private void logMessage(Level level, String msg) {
        if (logger != null) {
            logger.log(level, msg);
        }
    }

    private SignatureDetails getSignatureDetails(Request request) {
        SignatureDetails sigDetails =
            (request.getIsRefresh() ? refreshSigDetails : currentSigDetails);
        if (sigDetails != null) {
            return sigDetails;
        }

        if (request.getIsRefresh()) {
            /* try current details before failing */
            sigDetails = currentSigDetails;
            if (sigDetails != null) {
                return sigDetails;
            }
        }

        return getSignatureDetailsInternal(false);
    }

    /* visible for testing */
    synchronized SignatureDetails getSignatureDetailsInternal(boolean isRefresh)
    {
        /*
         * add one minute to the current time, so that any caching is
         * effective over a more valid time period.
         */
        long nowPlus = System.currentTimeMillis() + 60_000L;
        String date = createFormatter().format(new Date(nowPlus));
        String keyId = provider.getKeyId();
        if (provider instanceof InstancePrincipalsProvider) {
            privateKeyProvider.reload(provider.getPrivateKey(),
                                      provider.getPassphraseCharacters());
        }
        String signature;
        try {
            signature = sign(signingContent(date), privateKeyProvider.getKey());
        } catch (Exception e) {
            logMessage(Level.SEVERE, "Error signing request " + e.getMessage());
            return null;
        }

        String signingHeader = (delegationToken == null)
            ? SIGNING_HEADERS : SIGNING_HEADERS_WITH_OBO;
        String sigHeader = String.format(SIGNATURE_HEADER_FORMAT,
                                         signingHeader,
                                         keyId,
                                         RSA,
                                         signature,
                                         SINGATURE_VERSION);
        SignatureDetails sigDetails = new SignatureDetails(sigHeader, date);
        if (!isRefresh) {
            /*
             * if this is not a refresh, use the normal key and schedule a
             * refresh
             */
            currentSigDetails = sigDetails;
            scheduleRefresh();
        } else {
            /*
             * If this is a refresh put the object in a temporary key.
             * The caller (the refresh task) will:
             * 1. perform callbacks if needed and when done,
             * 2. move the object to the normal key and schedule a refresh
             */
            refreshSigDetails = sigDetails;
        }
        return sigDetails;
    }

    private synchronized void setRefreshKey() {
        if (refreshSigDetails != null) {
            currentSigDetails = refreshSigDetails;
            refreshSigDetails = null;
        }
    }

    String signingContent(String date) {
        StringBuilder sb = new StringBuilder();
        sb.append(REQUEST_TARGET).append(HEADER_DELIMITER)
        .append("post /").append(NOSQL_DATA_PATH).append("\n")
        .append(HOST).append(HEADER_DELIMITER)
        .append(serviceHost).append("\n")
        .append(DATE).append(HEADER_DELIMITER).append(date);

        if (delegationToken != null) {
            sb.append("\n").append(OBO_TOKEN_HEADER)
              .append(HEADER_DELIMITER).append(delegationToken);
        }
        return sb.toString();
    }

    private void scheduleRefresh() {
        /* If refresh interval is 0, don't schedule a refresh */
        if (refreshIntervalMs == 0) {
            return;
        }
        if (refresher != null) {
            refresher.cancel();
            refresher = null;
        }

        refresher = new Timer(true /* isDaemon */);
        refresher.schedule(new RefreshTask(), refreshIntervalMs);
    }

    private static String readFile(File file)
        throws IOException {

        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file),
                                      StandardCharsets.UTF_8))) {

           String line = null;
           while ((line = reader.readLine()) != null) {
               sb.append(line);
           }
        }
        return sb.toString();
    }

    private class RefreshTask extends TimerTask {
        private static final int DELAY_MS = 500;

        private void handleRefreshCallback(long refreshMs) {
            if (refreshSigDetails == null) {
                logMessage(Level.FINE,
                           "Refresh didn't find cached refresh key");
                return;
            }

            if (onSigRefresh != null) {
                /* don't let problems in the callback affect this path */
                try {
                    onSigRefresh.refresh(refreshMs);
                } catch (Throwable t) {
                    logMessage(Level.FINE,
                               "Exception from OnSignatureRefresh: " + t);
                }
            }

            /* move temporary object to the normal key */
            setRefreshKey();
            scheduleRefresh();
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            Exception lastException;
            do {
                try {
                    getSignatureDetailsInternal(true);
                    handleRefreshCallback(refreshAheadMs);
                    return;
                } catch (SecurityInfoNotReadyException se) {
                    /*
                     * This exception is thrown only if instance principal
                     * is configured to use, indicates the last attempt of
                     * security token acquisition is failed.
                     */
                    lastException = se;
                    try {
                        Thread.sleep(DELAY_MS);
                    } catch (InterruptedException ie) {}
                    continue;
                } catch (Exception e) {
                    lastException = e;
                    break;
                }
            } while ((System.currentTimeMillis() - startTime) < refreshAheadMs);

            /*
             * Ignore the failure of refresh. The driver would try to
             * generate signature in the next request if signature is not
             * available, the failure would be reported at that moment.
             */
            logMessage(Level.WARNING,
                       "Unable to refresh cached request signature, " +
                       lastException.getMessage());
            refresher.cancel();
            refresher = null;
        }
    }

    /**
     * @hidden
     * Internal use only.
     * <p>
     * Sets a signature refresh callback to be called when the signature
     * is refreshed
     * @param onSigRefresh the callback
     */
    public void setOnSignatureRefresh(OnSignatureRefresh onSigRefresh) {
        this.onSigRefresh = onSigRefresh;
    }

    /**
     * @hidden
     * Internal use only.
     * <p>
     * Returns the signature refresh callback, or null if not set
     * @return the callback or null
     */
    public OnSignatureRefresh getOnSignatureRefresh() {
        return onSigRefresh;
    }

    static class SignatureDetails {

        /* Signature header string */
        private String signatureHeader;

        /*
         * Signing date, keep it and pass along with each request,
         * so requests can reuse the signature within the 5-mins
         * time window.
         */
        private String date;

        SignatureDetails(String signatureHeader,
                         String date) {
            this.signatureHeader = signatureHeader;
            this.date = date;
        }

        String getDate() {
            return date;
        }

        String getSignatureHeader() {
            return signatureHeader;
        }
   }

    /**
     * Claim keys in the resource principal session token(RPST).
     * <p>
     * They can be used to retrieve resource principal metadata such as its
     * compartment and tenancy OCID.
     */
    public static class ResourcePrincipalClaimKeys {
        /**
         * The claim name that the RPST holds for the resource compartment.
         * This can be passed to
         * {@link SignatureProvider#getResourcePrincipalClaim(String)}
         * to retrieve the resource's compartment OCID.
         */
        public final static String COMPARTMENT_ID_CLAIM_KEY = "res_compartment";

        /**
         * The claim name that the RPST holds for the resource tenancy.
         * This can be passed to
         * {@link SignatureProvider#getResourcePrincipalClaim(String)}
         * to retrieve the resource's tenancy OCID.
         */
        public final static String TENANT_ID_CLAIM_KEY = "res_tenant";
    }
}
