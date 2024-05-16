/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
import static oracle.nosql.driver.iam.Utils.computeBodySHA256;
import static oracle.nosql.driver.iam.Utils.sign;
import static oracle.nosql.driver.util.HttpConstants.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

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

    private static final String SIGNING_HEADERS_WITH_CONTENT =
        "(request-target) host date " +
        "content-length content-type x-content-sha256";
    private static final String SIGNING_HEADERS_WITH_CONTENT_OBO =
        "(request-target) host date " +
        "content-length content-type x-content-sha256 opc-obo-token";
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
    private final AtomicReference<SignatureDetails> currentSigDetails =
            new AtomicReference<>();

    /* Scheduled Refresh task */
    private final AtomicReference<Disposable> refreshTask =
            new AtomicReference<>();

    /* A Flag to indicate whether signature flush is in progress */
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    /* Thread to run refresh task */
    private final Scheduler refreshScheduler = Schedulers.newSingle("token" +
            "-refresh", true);

    /* Refresh time before signature expires */
    private long refreshAheadMs = DEFAULT_REFRESH_AHEAD;

    /* Refresh interval, if zero, no refresh will be scheduled */
    private final long refreshIntervalMs;

    private String serviceHost;
    private final Region region;
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

    /**
     * Creates a SignatureProvider using a temporary session token read from
     * a token file. The path of token file is read from the default profile
     * in configuration file at the default location, the value of field
     * <code>security_token_file</code>. The configuration file used is
     * <code>~/.oci/config</code>. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_session_token">Session Token-Based Authentication</a>
     * for more details of session-token-based authentication.
     * <p>
     * You can use the OCI CLI to authenticate and create a token, see
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm">Token-based Authentication for the CLI</a>.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithSessionToken() {
        SignatureProvider provider = new SignatureProvider(
            new SessionTokenProvider());
        return provider;
    }

    /**
     * Creates a SignatureProvider using a temporary session token read from
     * a token file. The path of token file is read from the specified profile
     * in configuration file at the default location, the value of field
     * <code>security_token_file</code>. The configuration file used is
     * <code>~/.oci/config</code>. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_session_token">Session Token-Based Authentication</a>
     * for more details of session-token-based authentication.
     * <p>
     * You can use the OCI CLI to authenticate and create a token, see
     * <a href="https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm">Token-based Authentication for the CLI</a>.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param profile profile name used to load session token
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithSessionToken(String profile) {
        SignatureProvider provider = new SignatureProvider(
            new SessionTokenProvider(profile));
        return provider;
    }

    /**
     * Creates a SignatureProvider using a temporary session token read from
     * a token file. The path of token file is read from the specified profile
     * in configuration file at the specified location, the value of field
     * <code>security_token_file</code>. The configuration file
     * used is <code>~/.oci/config</code>. See
     * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm">SDK Configuration File</a>
     * for details of the file's contents and format.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_session_token">Session Token-Based Authentication</a>
     * for more details of session-token-based authentication.
     * <p>
     * You can use the OCI CLI to authenticate and create a token, see
     * <a href="https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm">Token-based Authentication for the CLI</a>.
     * <p>
     * When using this constructor the user has a default compartment for
     * all tables. It is the root compartment of the user's tenancy.
     *
     * @param configFilePath path of configuration file
     *
     * @param profile profile name used to load session token
     *
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithSessionToken(String configFilePath, String profile) {
        SignatureProvider provider = new SignatureProvider(
            new SessionTokenProvider(configFilePath, profile));
        return provider;
    }

    /**
     * Creates a SignatureProvider with Container Engine for Kubernetes (OKE)
     * workload identity using the Kubernetes service account token at the
     * default path
     * <code>/var/run/secrets/kubernetes.io/serviceaccount/token</code>.
     * This provider can only be used inside Kubernetes pods.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm">Granting Workloads Access to OCI Resources</a>
     * for more details of OKE workload identity.
     *
     * @return SignatureProvider
     */
    public static SignatureProvider createWithOkeWorkloadIdentity() {
        SignatureProvider provider = new SignatureProvider(
            new OkeWorkloadIdentityProvider(null, null, null));
        return provider;
    }

    /**
     * Creates a SignatureProvider with Container Engine for Kubernetes (OKE)
     * workload identity using specified Kubernetes service account token string.
     * If token string is null, the provider will use the service account token
     * at the default path
     * <code>/var/run/secrets/kubernetes.io/serviceaccount/token</code>.
     * This provider can only be used inside Kubernetes pods.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm">Granting Workloads Access to OCI Resources</a>
     * for more details of OKE workload identity.
     *
     * @param serviceAccountToken Kubernetes service account token string
     * @param logger the logger used by the SignatureProvider
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithOkeWorkloadIdentity(String serviceAccountToken,
                                      Logger logger) {
        SignatureProvider provider = new SignatureProvider(
            new OkeWorkloadIdentityProvider(serviceAccountToken, null, logger));
        return provider;
    }

    /**
     * Creates a SignatureProvider with Container Engine for Kubernetes (OKE)
     * workload identity using Kubernetes service account token in the specified
     * token file. If token file is null, the provider will use the service
     * account token at the default path
     * <code>/var/run/secrets/kubernetes.io/serviceaccount/token</code>.
     * This provider can only be used inside Kubernetes pods.
     * <p>
     * See <a href="https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contenggrantingworkloadaccesstoresources.htm">Granting Workloads Access to OCI Resources</a>
     * for more details of OKE workload identity.
     *
     * @param serviceAccountTokenFile Kubernetes service account token file
     * @param logger the logger used by the SignatureProvider
     * @return SignatureProvider
     */
    public static SignatureProvider
        createWithOkeWorkloadIdentity(File serviceAccountTokenFile,
                                      Logger logger) {
        SignatureProvider provider = new SignatureProvider(
            new OkeWorkloadIdentityProvider(
                null, serviceAccountTokenFile, logger));
        return provider;
    }

    /**
     * Constructor for SignatureProvider given an
     * AuthenticationProfileProvider.
     * This is for advanced use only; use of the create* methods is preferred.
     * The SignatureProvider that generates and caches request signature using
     * key id and private key supplied by {@link AuthenticationProfileProvider}.
     *
     * @param provider The provider to use
     */
    public SignatureProvider(AuthenticationProfileProvider provider) {
        this(provider, MAX_ENTRY_LIFE_TIME, DEFAULT_REFRESH_AHEAD);
    }

    /**
     * Constructor for SignatureProvider given an
     * AuthenticationProfileProvider and refresh details.
     * This is for advanced use only; use of the create* methods is preferred.
     * The constructor that is able to set refresh time before signature
     * expires.
     *
     * @param profileProvider The provider to use
     * @param durationSeconds amount of time to keep signature before refresh
     * @param refreshAheadMs how soon before expiry to start a new refresh
     */
    public SignatureProvider(AuthenticationProfileProvider profileProvider,
                                int durationSeconds,
                                int refreshAheadMs) {
        this.region = (profileProvider instanceof RegionProvider) ?
                ((RegionProvider) profileProvider).getRegion() : null;
        this.provider = profileProvider;
        this.privateKeyProvider = (provider == null) ?
            null : new PrivateKeyProvider(provider);

        if (durationSeconds > MAX_ENTRY_LIFE_TIME) {
            throw new IllegalArgumentException(
                "Signature cannot be cached longer than " +
                MAX_ENTRY_LIFE_TIME + " seconds");
        }

        this.refreshAheadMs = refreshAheadMs;
        long durationMS = durationSeconds * 1000L;
        if (durationMS > refreshAheadMs) {
            this.refreshIntervalMs = durationMS - refreshAheadMs;
        } else {
            this.refreshIntervalMs = 0;
        }
        if (this.provider instanceof SecurityTokenBasedProvider) {
            ((SecurityTokenBasedProvider) provider)
                .setMinTokenLifetime(durationMS);
        }
    }

    @Override
    public Mono<String> getAuthorizationStringAsync(Request request) {
        if (serviceHost == null) {
            Mono.error(new IllegalArgumentException(
                    "Unable to find service host, use setServiceHost " +
                            "to load from NoSQLHandleConfig"));
        }
        return getSignatureDetails(request).map(SignatureDetails::getSignatureHeader);
    }

    @Override
    public String getAuthorizationString(Request request) {
        return getAuthorizationStringAsync(request).block();
    }

    @Override
    public Mono<Void> setRequiredHeadersAsync(String authString,
                                   Request request,
                                   HttpHeaders headers,
                                   byte[] content) {
        Mono<SignatureDetails> sigDetailsMono = (content != null) ?
                getSignatureWithContent(request, headers, content) :
                getSignatureDetails(request);
        return sigDetailsMono.doOnNext(sigDetails -> {
            headers.add(AUTHORIZATION, sigDetails.getSignatureHeader());
            headers.add(DATE, sigDetails.getDate());

            final String token = getDelegationToken(request);
            if (token != null) {
                headers.add(OBO_TOKEN_HEADER, token);
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
        }).then();
    }

    public void setRequiredHeaders(String authString,
                                   Request request,
                                   HttpHeaders headers,
                                   byte[] content) {
        setRequiredHeadersAsync(authString,request,headers,content).block();
    }


    @Override
    public void flushCache() {
        /* If already a flush in progress do nothing */
        if (!flushInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            /* cancel scheduled task if any */
            Disposable task = refreshTask.getAndSet(null);
            if (task != null) {
                logger.fine("Flush: disposing refresh task");
                task.dispose();
            }
            /* get new signature */
            getSignatureDetailsForCache(false).subscribe();
        } finally {
            flushInProgress.set(false);
        }
    }

    /**
     * Get tenant OCID if using user principal.
     * @return tenant OCID of user
     */
    private String getTenantOCID() {
        if (provider instanceof UserAuthenticationProfileProvider) {
            return ((UserAuthenticationProfileProvider)this.provider)
                .getTenantId();
        } else if (provider instanceof SessionTokenProvider) {
            return ((SessionTokenProvider) this.provider).getTenantId();
        }
        return null;
    }

    @Override
    public void close() {
        Disposable task = refreshTask.getAndSet(null);
        if(task != null) {
            task.dispose();
        }
        if (provider instanceof SecurityTokenBasedProvider) {
            ((SecurityTokenBasedProvider)this.provider).close();
        }
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public boolean forCloud() {
        return true;
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
        if (provider instanceof SecurityTokenBasedProvider) {
            ((SecurityTokenBasedProvider)this.provider).prepare(config);
        }

        /* creates and caches a signature as warm-up */
        getSignatureDetailsForCache(false).block();
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

    private Mono<SignatureDetails> getSignatureDetails(Request request) {
        return Mono.just(currentSigDetails.get());
    }

    private Mono<SignatureDetails> getSignatureWithContent(Request request,
                                                     HttpHeaders headers,
                                                     byte[] content) {
        return getSignatureDetailsInternal(false, request, headers, content);
    }

    synchronized Mono<Void>
    getSignatureDetailsForCache(boolean isRefresh) {
        return getSignatureDetailsInternal(isRefresh,
                null /* request */,
                null /* headers */,
                null /* content */)
                .doOnNext(signatureDetails -> {
                    logger.fine("Successfully Got new token");
                    currentSigDetails.set(signatureDetails);
                    scheduleRefresh();
                }).then();
    }

    /* visible for testing */
    synchronized Mono<SignatureDetails>
        getSignatureDetailsInternal(boolean isRefresh,
                                    Request request,
                                    HttpHeaders headers,
                                    byte[] content) {
        return Mono.fromCallable(() -> {
            /*
             * add one minute to the current time, so that any caching is
             * effective over a more valid time period.
             */
            long nowPlus = System.currentTimeMillis() + 60_000L;
            String date = createFormatter().format(new Date(nowPlus));
            String keyId = provider.getKeyId();

            /*
             * Security token based providers may refresh the security token
             * and associated private key in above getKeyId() method, reload
             * private key to PrivateKeyProvider to avoid a mismatch, which
             * will create an invalid signature, cause authentication error.
             */
            if (provider instanceof SecurityTokenBasedProvider) {
                privateKeyProvider.reload(provider.getPrivateKey(),
                        provider.getPassphraseCharacters());
            }
            String signature;
            try {
                signature = sign(signingContent(date, request, headers, content),
                        privateKeyProvider.getKey());
            } catch (Exception e) {
                logMessage(Level.SEVERE, "Error signing request " + e.getMessage());
                return null;
            }

            String token = getDelegationToken(request);
            String signingHeader;
            if (content != null) {
                signingHeader = (token == null)
                        ? SIGNING_HEADERS_WITH_CONTENT :
                        SIGNING_HEADERS_WITH_CONTENT_OBO;
            } else {
                signingHeader = (token == null)
                        ? SIGNING_HEADERS : SIGNING_HEADERS_WITH_OBO;
            }

            String sigHeader = String.format(SIGNATURE_HEADER_FORMAT,
                    signingHeader,
                    keyId,
                    RSA,
                    signature,
                    SINGATURE_VERSION);
            SignatureDetails sigDetails = new SignatureDetails(sigHeader, date);
            return sigDetails;
        }).retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                .filter(throwable -> throwable instanceof SecurityInfoNotReadyException))
        /* This blocks the thread. Subscribe on boundedElastic */
        .subscribeOn(Schedulers.boundedElastic());
    }

    /*
     * Since Request.oboToken is not required by non Java SDKs, if there
     * is no Request.oboToken, simplify this method as following:
     *
     * private String getDelegationToken(Request req) {
     *     return delegationToken;
     * }
     */
    private String getDelegationToken(Request req) {
        return (req != null && req.getOboToken() != null) ?
               req.getOboToken() : delegationToken;
    }

    private String signingContent(String date,
                                  Request request,
                                  HttpHeaders headers,
                                  byte[] content) {
        StringBuilder sb = new StringBuilder();
        sb.append(REQUEST_TARGET).append(HEADER_DELIMITER)
          .append("post /").append(NOSQL_DATA_PATH).append("\n")
          .append(HOST).append(HEADER_DELIMITER)
          .append(serviceHost).append("\n")
          .append(DATE).append(HEADER_DELIMITER).append(date);

        if (content != null) {
            /* Must be in this order */
            sb.append("\n").append("content-length")
              .append(HEADER_DELIMITER).append(headers.get(CONTENT_LENGTH))
              .append("\n").append("content-type")
              .append(HEADER_DELIMITER).append(headers.get(CONTENT_TYPE));

            String sha256 = computeBodySHA256(content);
            sb.append("\n").append("x-content-sha256")
              .append(HEADER_DELIMITER).append(sha256);
            headers.add("x-content-sha256", sha256);
        }

        String token = getDelegationToken(request);
        if (token != null) {
            sb.append("\n").append(OBO_TOKEN_HEADER)
              .append(HEADER_DELIMITER).append(token);
        }
        return sb.toString();
    }

    private void scheduleRefresh() {
        /* If refresh interval is 0, don't schedule a refresh */
        if (refreshIntervalMs == 0) {
            return;
        }
        Disposable task = refreshTask.getAndSet(null);
        if (task != null) {
            logger.fine("disposing refresh task");
            task.dispose();
        }
        task = refreshScheduler.schedule(() -> {
            logger.fine("refreshing the token");
            getSignatureDetailsForCache(false)
                    .subscribe();
        }, refreshIntervalMs, TimeUnit.MILLISECONDS);
        refreshTask.set(task);
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

    static class SignatureDetails {

        /* Signature header string */
        private final String signatureHeader;

        /*
         * Signing date, keep it and pass along with each request,
         * so requests can reuse the signature within the 5-mins
         * time window.
         */
        private final String date;

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
