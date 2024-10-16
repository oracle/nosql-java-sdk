/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * Represents the argument of a {@link NoSQLHandle#listTables} operation which
 * lists all available tables associated with the identity associated with the
 * handle used for the operation. If the list is large it can be paged by using
 * the startIndex and limit parameters. The list is returned in a simple
 * array in {@link ListTablesResult}. Names are returned sorted in alphabetical
 * order in order to facilitate paging.
 * @see NoSQLHandle#listTables
 */
public class ListTablesRequest extends Request {
    private int startIndex;
    private int limit;
    private String namespace;

    /**
     * Cloud service only.
     * <p>
     * Sets the name or id of a compartment to be used for this operation.
     * <p>
     * The compartment may be specified as either a name (or path for nested
     * compartments) or as an id (OCID). A name (vs id) can only
     * be used when authenticated using a specific user identity. It is
     * <b>not</b> available if authenticated as an Instance Principal which can
     * be done when calling the service from a compute instance in the Oracle
     * Cloud Infrastructure.  See {@link
     * SignatureProvider#createWithInstancePrincipal}
     *
     * @param compartment the name or id. If using a nested compartment,
     * specify the full compartment path
     * <code>compartmentA.compartmentB</code>, but exclude the name of the
     * root compartment (tenant).
     *
     * @return this
     */
    public ListTablesRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the maximum number of table names to return in the operation. If
     * not set (0) there is no application-imposed limit.
     *
     * @return the maximum number of tables to return in a single request
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Returns the index to use to start returning table names. This is related
     * to the {@link ListTablesResult#getLastReturnedIndex} from a previous
     * request and can be used to page table names. If not set, the list starts
     * at index 0.
     *
     * @return the start index.
     */
    public int getStartIndex() {
        return startIndex;
    }

    /**
     * Sets the maximum number of table names to return in the operation. If
     * not set (0) there is no limit.
     *
     * @param limit the maximum number of tables
     *
     * @return this
     */
    public ListTablesRequest setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Sets the index to use to start returning table names. This is related
     * to the {@link ListTablesResult#getLastReturnedIndex} from a previous
     * request and can be used to page table names. If not set, the list starts
     * at index 0.
     *
     * @param startIndex the start index
     *
     * @return this
     */
    public ListTablesRequest setStartIndex(int startIndex) {
        this.startIndex = startIndex;
        return this;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public ListTablesRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * On-premises only.
     * <p>
     * Sets the namespace to use for the list. If not set all tables accessible
     * to the user will be returned. If set, only tables in the namespace
     * provided are returned.
     *
     * @param namespace the namespace to use
     *
     * @return this
     *
     * @since 5.4.10
     */
    public ListTablesRequest setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * On-premises only.
     * <p>
     * Returns the namespace to use for the list or null if not set.
     *
     * @return the namespace
     */
    @Override
    public String getNamespace() {
        return namespace;
    }

    /**
     * @hidden
     */
    @Override
    public  void validate() {
        if (startIndex < 0 || limit < 0) {
            throw new IllegalArgumentException(
                "ListTables: start index and number of tables must be " +
                "non-negative");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createListTablesSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createListTablesDeserializer();
    }

    @Override
    public String getTypeName() {
        return "ListTables";
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
