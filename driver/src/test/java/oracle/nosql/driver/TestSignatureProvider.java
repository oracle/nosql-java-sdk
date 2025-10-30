/*-
 * Copyright (C) 2011, 2019 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.HttpConstants.REQUEST_COMPARTMENT_ID;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.Request;

import io.netty.handler.codec.http.HttpHeaders;

public class TestSignatureProvider extends SignatureProvider {
    public static String TENANT_ID = "TestTenant";
    public static String USER_ID = "TestUser";

    private final String userId;
    private String tenantId;

    public TestSignatureProvider() {
        this(USER_ID, TENANT_ID);
    }

    public TestSignatureProvider(String userId, String tenantId) {
        super(null, 0, 0);
        this.userId = userId;
        this.tenantId = tenantId;
    }

    public TestSignatureProvider setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    @Override
    public String getAuthorizationString(Request request) {
        return "Signature " + tenantId + ":" + userId;
    }

    @Override
    public void setRequiredHeaders(String authString,
                                   Request request,
                                   HttpHeaders headers,
                                   byte[] content) {
        String compartmentId = request.getCompartment();
        if (compartmentId == null) {
            /*
             * If request doesn't has compartment id, set the tenant id as the
             * default compartment, which is the root compartment in IAM if
             * using user principal.
             */
            compartmentId = tenantId;
        }
        if (compartmentId != null) {
            headers.add(REQUEST_COMPARTMENT_ID, compartmentId);
        }
        headers.add(AUTHORIZATION, getAuthorizationString(null));
    }

    @Override
    public void close() {
    }

    /**
     * @since 5.2.27, prepare would throw NPE without specifying
     * AuthenticationProfileProvider to SignatureProvider.
     */
    @Override
    public SignatureProvider prepare(NoSQLHandleConfig config) {
        return this;
    }
}
