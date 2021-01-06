/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.driver.BatchOperationNumberLimitException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * Represents the input to a {@link NoSQLHandle#writeMultiple} operation.
 *
 * This request can be used to perform a sequence of {@link PutRequest} or
 * {@link DeleteRequest} operations associated with a table that share the same
 * <em>shard key</em> portion of their primary keys, the WriteMultiple
 * operation as whole is atomic. It is an efficient way to atomically modify
 * multiple related rows.
 * <p>
 * On a successful operation {@link WriteMultipleResult#getSuccess} returns
 * true. The execution result of each operations can be retrieved using
 * {@link WriteMultipleResult#getResults}.
 * <p>
 * If the WriteMultiple operation is aborted because of the failure of an
 * operation with abortIfUnsuccessful set to true, then
 * {@link WriteMultipleResult#getSuccess} return false, the index of failed
 * operation can be accessed using
 * {@link WriteMultipleResult#getFailedOperationIndex}, and the execution
 * result of failed operation can be accessed using
 * {@link WriteMultipleResult#getFailedOperationResult()}.
 * @see NoSQLHandle#writeMultiple
 */
public class WriteMultipleRequest extends Request {

    /* The list of requests */
    private final List<OperationRequest> operations;

    /**
     * Constructs an empty request
     */
    public WriteMultipleRequest() {
        operations = new ArrayList<OperationRequest>();
    }

    /**
     * Adds a Request to the operation list.
     *
     * @param request the Request to add, either {@link PutRequest} or
     * {@link DeleteRequest}.
     *
     * @param abortIfUnsuccessful is true if this operation should cause the
     * entire WriteMultiple operation to abort when this operation fails.
     *
     * @return this
     *
     * @throws BatchOperationNumberLimitException if the number of
     * requests exceeds the limit, or IllegalArgumentException if the request
     * is neither a {@link PutRequest} or {@link DeleteRequest}. or any invalid
     * state of the Request.
     */
    public WriteMultipleRequest add(Request request,
                                    boolean abortIfUnsuccessful) {
        addRequest(request, abortIfUnsuccessful);
        return this;
    }

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
    public WriteMultipleRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the timeout to use for the operation, in milliseconds. A value
     * of 0 indicates that the timeout has not been set.
     *
     * @return the value
     */
    public int getTimeout() {
        return super.getTimeoutInternal();
    }

    /**
     * Returns the number of Requests.
     *
     * @return the number of Requests
     */
    public int getNumOperations() {
        return operations.size();
    }

    /**
     * Returns the Request at the given position, it may be either a
     * {@link PutRequest} or a {@link DeleteRequest} object.
     *
     * @param index the position of Request to get
     *
     * @return the Request at the given position
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of Requests.
     */
    public Request getRequest(int index) {
        return operations.get(index).getRequest();
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set in {@link NoSQLHandleConfig}. The value must be
     * positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public WriteMultipleRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Removes all of the operations from the WriteMultiple request.
     */
    public void clear() {
        super.setTableNameInternal(null);
        operations.clear();
    }

    /**
     * @hidden
     * @param factory the factory
     * @return the Serializer
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createWriteMultipleSerializer();
    }

    /**
     * @hidden
     * @param factory the factory
     * @return the Deserializer
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createWriteMultipleDeserializer();
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (operations.isEmpty()) {
            throw new IllegalArgumentException("The requests list is empty");
        }
    }

    /**
     * Adds an operation to the list, do validation check before adding it.
     */
    private void addRequest(Request request, boolean abortIfUnsuccessful) {

        if (!(request instanceof PutRequest) &&
            !(request instanceof DeleteRequest)) {
            throw new IllegalArgumentException("Invalid request, only " +
                "PutRequest or DeleteRequest is allowed to add: " + request);
        }

        WriteRequest wrReq = (WriteRequest)request;
        if (tableName == null) {
            tableName = wrReq.getTableName();
        } else {
            if (!wrReq.getTableName().equalsIgnoreCase(tableName)) {
                throw new IllegalArgumentException("The tableName used for " +
                    "the operation is different from that of others: " +
                    tableName);
            }
        }

        request.validate();
        operations.add(new OperationRequest(wrReq, abortIfUnsuccessful));
    }

    /**
     * @hidden
     * Internal use only
     *
     * Returns the request lists
     * @return the operations
     */
    public List<OperationRequest> getOperations() {
        return operations;
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        int numops = operations.size();
        for (int x=0; x<numops; x++) {
            Request r = operations.get(x).getRequest();
            if (r.doesReads()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesWrites() {
        return true;
    }

    /**
     * @hidden
     * Internal use only
     *
     * A wrapper of WriteRequest that contains an additional flag
     * abortIfUnsuccessful.
     */
    public static class OperationRequest {
        private final boolean abortIfUnsuccessful;
        private final WriteRequest request;

        OperationRequest(WriteRequest request, boolean abortIfUnsuccessful) {
            this.request = request;
            this.abortIfUnsuccessful = abortIfUnsuccessful;
        }

        public boolean isAbortIfUnsuccessful() {
            return abortIfUnsuccessful;
        }

        public WriteRequest getRequest() {
            return request;
        }
    }
}
