/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
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
 * package are used to generate request signature to the driver for each
 * operation.
 * <p>
 * {@link oracle.nosql.driver.iam.SignatureProvider} is the primary instance
 * that takes authentication information supplied by above providers to generate
 * request signature for each operation.
 */
package oracle.nosql.driver.iam;
