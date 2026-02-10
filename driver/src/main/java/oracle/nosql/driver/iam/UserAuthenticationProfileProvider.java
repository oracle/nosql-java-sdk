/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam;

/**
 * @hidden
 * Internal use only
 * <p>
 * <p>
 * The provider to supply user's authentication profile.
 */
interface UserAuthenticationProfileProvider
    extends AuthenticationProfileProvider {

    /**
     * Returns the fingerprint of the key being used.
     *
     * @return The fingerprint.
     */
    String getFingerprint();

    /**
     * Returns the tenant OCID.
     *
     * @return The tenant OCID.
     */
    String getTenantId();

    /**
     * Returns the user OCID.
     *
     * @return The user OCID.
     */
    String getUserId();
}
