/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
/**
 * This package contains the public API for managing security for the
 * Oracle NoSQL Database Cloud using Oracle Cloud Infrastructure Identity
 * and Access Management (IAM). It is specific to the Cloud Service.
 * <p>
 * All operations require a request signature that is used by the system to
 * authorize the operation. Managing IAM identities and policies is outside
 * of the scope of this documentation. The interfaces and classes in this
 * package are used to generate a request signature for each operation.
 * <p>
 * {@link oracle.nosql.driver.iam.SignatureProvider} is the class that uses
 * authentication information of user, instance or resource principal to
 * generate a request signature for each operation.
 * <p>
 * The request signature produced by provider comprises the following headers:
 * <ol>
 * <li>(request-target)</li>
 * <li>host</li>
 * <li>date</li>
 * <li>opc-obo-token (optional)</li>
 * </ol>
 * Note that <code>opc-obo-token</code> header is included only if the provider
 * is created with instance principal for delegation.
 * <p>
 * Example request signature:
 * <pre>Signature version="1",keyId="keyId",algorithm="rsa-sha256", headers="(request-target) host date",signature="Base64(RSA-SHA256(signing_string))"</pre>
 * <p>
 * SignatureProvider caches the request signature to improve the driver-side
 * performance. The default cache duration is 4 minutes. The cached signature
 * is refreshed asynchronously by a refresh task that is scheduled to renew
 * the cached signature 20 seconds ahead of the cache eviction.
 * <p>
 * When SignatureProvider is created with a user principal either using
 * OCI configuration file or passing user authentication information directly,
 * the request signature is calculated with user's private key. See
 * <a href="https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm">Required Keys and OCIDs</a>
 * for details of the required parameters.
 * <p>
 * SignatureProvider can also be created with instance or resource principal.
 * See <a href="https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm">Calling Services from Instances</a>
 * and
 * <a href="https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm">Accessing Other Oracle Cloud Infrastructure Resources from Running Functions</a>
 * for details of the required configurations. When using instance or resource
 * principal, the request signature is created with a security token issued by
 * IAM. The security token is cached and refreshed by the provider transparently.
 * When refreshing or fetching the cached signature, the security token is
 * refreshed if token is close to its expiration time, by default 4 minutes
 * ahead. If the lifetime of the security token is shorter than 4 minutes,
 * the provider will always refresh at the token half-life.
 */
package oracle.nosql.driver.iam;
