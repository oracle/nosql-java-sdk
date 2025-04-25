package oracle.nosql.driver.kv;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.ops.Request;
import io.netty.handler.codec.http.HttpHeaders;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;

import java.net.URI;
import java.util.concurrent.locks.ReentrantLock;

public class OAuthAccessTokenProvider implements AuthorizationProvider {

    private volatile BearerAccessToken accessToken;
    private volatile RefreshToken refreshToken;
    private final URI tokenEndpoint;
    private final ClientID clientId;
    private final Secret clientSecret;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile long tokenExpiryTimeMillis = 0;

    public OAuthAccessTokenProvider(String accessToken,
                                    String refreshToken,
                                    URI tokenEndpoint,
                                    String clientId,
                                    String clientSecret) {
        this.accessToken = new BearerAccessToken(accessToken);
        this.refreshToken = new RefreshToken(refreshToken);
        this.tokenEndpoint = tokenEndpoint;
        this.clientId = new ClientID(clientId);
        this.clientSecret = new Secret(clientSecret);
        this.tokenExpiryTimeMillis = System.currentTimeMillis() + this.accessToken.getLifetime() * 1000L;
    }

    @Override
    public String getAuthorizationString(Request request) {
        if (accessTokenNeedsRefresh()) {
            refreshAccessToken();
        }
        return "Bearer " + accessToken.getValue();
    }

    private boolean accessTokenNeedsRefresh() {
        return System.currentTimeMillis() > (tokenExpiryTimeMillis - 60000); // refresh 1 min early
    }

    private void refreshAccessToken() {
        lock.lock();
        try {
            if (!accessTokenNeedsRefresh()) return;

            TokenRequest tokenRequest = new TokenRequest(
                tokenEndpoint,
                new ClientSecretBasic(clientId, clientSecret),
                new RefreshTokenGrant(refreshToken));

            HTTPResponse response = tokenRequest.toHTTPRequest().send();
            TokenResponse tokenResponse = OIDCTokenResponseParser.parse(response);

            if (!tokenResponse.indicatesSuccess()) {
                throw new RuntimeException("Token refresh failed: " +
                        tokenResponse.toErrorResponse().getErrorObject());
            }

            OIDCTokenResponse success = (OIDCTokenResponse) tokenResponse.toSuccessResponse();
            Tokens tokens = success.getTokens();
            this.accessToken = (BearerAccessToken) tokens.getAccessToken();
            this.tokenExpiryTimeMillis = System.currentTimeMillis() + this.accessToken.getLifetime() * 1000L;

            RefreshToken newRefreshToken = tokens.getRefreshToken();
            if (newRefreshToken != null) {
                this.refreshToken = newRefreshToken;
            }

        } catch (Exception e) {
            throw new RuntimeException("Error refreshing OAuth access token", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void validateAuthString(String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Access token must not be null or empty");
        }
    }

    @Override
    public void setRequiredHeaders(String authString, Request request, HttpHeaders headers, byte[] content) {
        if (authString != null && !authString.isEmpty()) {
            headers.set(AUTHORIZATION, authString);
        }
    }

    @Override
    public void close() {
        // Nothing to close for now
    }

}
