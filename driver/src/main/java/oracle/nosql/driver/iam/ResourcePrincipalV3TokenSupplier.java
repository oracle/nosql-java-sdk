package oracle.nosql.driver.iam;

import static oracle.nosql.driver.iam.Utils.logTrace;
import static oracle.nosql.driver.iam.Utils.parseResourcePrincipalTokenResponse;
import static oracle.nosql.driver.iam.Utils.base64EncodeNoChunking;
import static oracle.nosql.driver.iam.Utils.isBlank;
import static oracle.nosql.driver.util.HttpRequestUtil.HttpResponse;
import static oracle.nosql.driver.iam.SecurityTokenSupplier.SecurityToken;

import java.net.URI;
import java.security.KeyPair;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;
import java.util.logging.Logger;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * This class gets a security token from the auth service by fetching the RPST1 and then passing
 * along the RPST1 to get RPT2 and further get security token RPST2 from the auth service, this
 * nested fetching of security token continues for 10 levels or when the opc-parent-url header in
 * the rpt response is the same as the rpt endpoint
 */
class ResourcePrincipalV3TokenSupplier extends AbstractTokenSupplier {
    private static final Logger logger =
            Logger.getLogger(ResourcePrincipalV3TokenSupplier.class.getName());
    private final String resourcePrincipalTokenUrl;
    private final ResourcePrincipalProvider leafAuthProvider;
    private final String OPC_PARENT_RPT_URL_HEADER = "opc-parent-rpt-url";

    /**
     * Constructor of ResourcePrincipalsTokenSupplier.
     *
     * @param resourcePrincipalTokenUrl             the direct url that can provide the resource principal
     *                                              token.
     * @param resourcePrincipalSessionTokenEndpoint the endpoint that can provide the resource
     *                                              principal session token.
     * @param sessionKeySupplier                    the session key supplier.
     * @param leafAuthProvider                      the auth provider for leaf resource
     */
    public ResourcePrincipalV3TokenSupplier(
            String resourcePrincipalTokenUrl,
            String resourcePrincipalSessionTokenEndpoint,
            SessionKeyPairSupplier sessionKeySupplier,
            ResourcePrincipalProvider leafAuthProvider) {
        super(
                resourcePrincipalTokenUrl,
                resourcePrincipalSessionTokenEndpoint,
                sessionKeySupplier,
                logger);

        this.resourcePrincipalTokenUrl = resourcePrincipalTokenUrl;
        this.leafAuthProvider = leafAuthProvider;
    }

    @Override
    public synchronized void close() {
        super.close();
        if (leafAuthProvider != null) {
            leafAuthProvider.close();
        }
    }

    @Override
    protected SecurityToken getSecurityTokenFromServer() {
        logTrace(logger, "Getting/Refreshing RPST leaf from the auth server");

        // Refresh and get resource principal token from identity
        keyId = leafAuthProvider.getKeyId();
        privateKeyProvider = new PrivateKeyProvider(leafAuthProvider);

        // use the refreshed session keys
        KeyPair keyPair = sessionKeyPairSupplier.getKeyPair();
        if (keyPair == null) {
            throw new IllegalArgumentException(
                    "Keypair for session was not provided");
        }

        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        if (publicKey == null) {
            throw new IllegalArgumentException("Public key is not present");
        }

        return getSecurityTokenFromServerInner(
                this.resourcePrincipalTokenUrl,
                publicKey,
                1);
    }

    protected SecurityToken getSecurityTokenFromServerInner(
            String lastResourcePrincipalTokenUrl,
            RSAPublicKey publicKey,
            int depth
    ) {
        final HttpResponse resourcePrincipalTokenResponse =
                getResourcePrincipalTokenResponse(null);
        final HttpHeaders headers = resourcePrincipalTokenResponse.getHeaders();

        // check for the parent rpt url in response headers
        String opcParentUrlHeader = null;
        if (headers != null && !headers.isEmpty()) {
            opcParentUrlHeader =
                    headers.get(OPC_PARENT_RPT_URL_HEADER) != null
                            ? headers.get(OPC_PARENT_RPT_URL_HEADER).trim()
                            : null;
        }

        final Map<String, String> resourcePrincipalTokenResponseBody =
                parseResourcePrincipalTokenResponse(
                        resourcePrincipalTokenResponse.getOutput());

        String resourcePrincipalToken =
                resourcePrincipalTokenResponseBody.get("resourcePrincipalToken");
        String servicePrincipalSessionToken =
                resourcePrincipalTokenResponseBody.get("servicePrincipalSessionToken");

        final SecurityToken securityToken = getResourcePrincipalSessionToken(
                resourcePrincipalToken,
                servicePrincipalSessionToken,
                base64EncodeNoChunking(publicKey)
        );

        // if depth is more than 10, return the security token obtained last
        if (depth > 9) return securityToken;

        // get the opc-parent-rpt-url header and check if matches the last one
        // if the opcParentUrlHeader matches the last rpt url, return the security token last
        // obtained
        if (isBlank(opcParentUrlHeader)
                || (!isBlank(opcParentUrlHeader)
                && opcParentUrlHeader.equalsIgnoreCase(lastResourcePrincipalTokenUrl))) {
            return securityToken;
        }

        ResourcePrincipalProvider tempAuthProvider = new ResourcePrincipalProvider(
                new FixedSecurityTokenSupplier(
                        sessionKeyPairSupplier, securityToken.getSecurityToken()),
                sessionKeyPairSupplier,
                leafAuthProvider.getRegion());

        // Update resource principal token and private key using newly formed auth provider.
        keyId = tempAuthProvider.getKeyId();
        privateKeyProvider = new PrivateKeyProvider(tempAuthProvider);

        // Free the client to release resources. We neither close the temp provider
        // as it's a FixedSecurityTokenSupplier resource principal provider and
        // doesn't contain a underlying http client, nor we close federation client
        // as the same client can be reused in all the iterations
        resourcePrincipalTokenClient.shutdown();

        resourcePrincipalTokenURI = URI.create(opcParentUrlHeader);
        resourcePrincipalTokenClient = buildHttpClient(resourcePrincipalTokenURI,
                        "resourcePrincipalTokenClient", logger);

        // recurse with the new value of the rpt url
        return getSecurityTokenFromServerInner(
                opcParentUrlHeader,
                publicKey,
                depth + 1);
    }
}
