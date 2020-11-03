/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.TableNotFoundException;

/**
 * TableResult is returned from {@link NoSQLHandle#getTable} and
 * {@link NoSQLHandle#tableRequest} operations. It encapsulates the
 * state of the table that is the target of the request.
 * <p>
 * Operations available in
 * {@link NoSQLHandle#tableRequest} such as table creation, modification, and
 * drop are asynchronous operations. When such an operation has been performend
 * it is necessary to call {@link NoSQLHandle#getTable} until the status of
 * the table is {@link State#ACTIVE} or there is an error condition. The
 * method {@link #waitForCompletion} exists to perform this task and should
 * be used to wait for an operation to complete.
 *<p>
 * {@link NoSQLHandle#getTable} is synchronous, returning static information
 * about the table as well as its current state.
 * @see NoSQLHandle#getTable
 * @see NoSQLHandle#tableRequest
 */
public class TableResult extends Result {
    /*
     * domainId is the compartment id (ocid)
     */
    private String domainId;
    private String tableName;
    private State state;
    private TableLimits limits;
    private String schema;
    private String operationId;

    /**
     * The current state of the table
     */
    public enum State {
        /**
         * The table is ready to be used. This is the steady state after
         * creation or modification.
         */
        ACTIVE,
        /**
         * The table is being created and cannot yet be used
         */
        CREATING,
        /**
         * The table has been dropped or does not exist
         */
        DROPPED,
        /**
         * The table is being dropped and cannot be used
         */
        DROPPING,
        /**
         * The table is being updated. It is available for normal use, but
         * additional table modification operations are not permitted
         * while the table is in this state.
         */
        UPDATING
    }

    /**
     * Returns the table state. A table in state {@link State#ACTIVE} or
     * {@link State#UPDATING} is usable for normal operation.
     *
     * @return the state
     */
    public State getTableState() {
        return state;
    }

    /**
     * Cloud service only.
     * <p>
     * Returns compartment id of the target table
     *
     * @return the domain
     */
    public String getCompartmentId() {
        return domainId;
    }

    /**
     * Returns the table name of the target table
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    public String getSchema() {
        return schema;
    }

    /**
     * Returns the throughput and capacity limits for the table.
     * Limits from an on-premise service will always be null.
     *
     * @return the limits
     */
    public TableLimits getTableLimits() {
        return limits;
    }

    /**
     * Returns the operation id for an asynchronous operation. This is null if
     * the request did not generate a new operation. The value can be used
     * in {@link GetTableRequest#setOperationId} to find potential errors
     * resulting from the operation.
     * @return the operation id, or null if not set
     */
    public String getOperationId() {
        return operationId;
    }

    /**
     * @hidden
     * @param operationId the operation id
     * @return this
     */
    public TableResult setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    /**
     * @hidden
     * @param state the table state
     * @return this
     */
    public TableResult setState(State state) {
        this.state = state;
        return this;
    }

    /**
     * @hidden
     * @param domainId the auth domain
     * @return this
     */
    public TableResult setDomainId(String domainId) {
        this.domainId = domainId;
        return this;
    }

    /**
     * @hidden
     * @param tableName the table name
     * @return this
     */
    public TableResult setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * @hidden
     * @param schema the schema
     * @return this
     */
    public TableResult setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    /**
     * @hidden
     * @param limits table limits
     * @return this
     */
    public TableResult setTableLimits(TableLimits limits) {
        this.limits = limits;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table ");
        sb.append(tableName).append("[")
            .append(state).append("] ").append(limits).append(" schema [" +
                                                              schema + "]")
            .append(" operationId = ").append(operationId);

        return sb.toString();
    }

    /**
     * @deprecated use {@link #waitForCompletion} instead.
     *
     * Waits for the specified table to reach the desired state. This is a
     * blocking, polling style wait that delays for the specified number of
     * milliseconds between each polling operation. The state of
     * {@link State#DROPPED} is treated specially in that it will be returned
     * as success, even if the table does not exist. Other states will throw
     * an exception if the table is not found.
     *
     * @param handle the NoSQLHandle to use
     * @param result a previously received TableResult
     * @param state the desired state
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @return the TableResult representing the table at the desired state
     *
     * @throws IllegalArgumentException if the parameters are not valid.
     *
     * @throws RequestTimeoutException if the operation times out.
     *
     * @throws NoSQLException if the operation id used is not null that the
     * operation has failed for some reason.
     */
    @Deprecated
    public static TableResult waitForState(NoSQLHandle handle,
                                           TableResult result,
                                           TableResult.State state,
                                           int waitMillis,
                                           int delayMillis) {

        return waitForState(handle,
                            result.getTableName(),
                            result.getCompartmentId(),
                            result.getOperationId(),
                            state,
                            waitMillis,
                            delayMillis);
    }

    /**
     * @deprecated use {@link #waitForCompletion} instead.
     *
     * Waits for the specified table to reach the desired state. This is a
     * blocking, polling style wait that delays for the specified number of
     * milliseconds between each polling operation. The state of
     * {@link State#DROPPED} is treated specially in that it will be returned
     * as success, even if the table does not exist. Other states will throw
     * an exception if the table is not found.
     *
     * @param handle the NoSQLHandle to use
     * @param tableName the table name
     * @param state the desired state
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @return the TableResult representing the table at the desired state
     *
     * @throws IllegalArgumentException if the parameters are not valid.
     *
     * @throws RequestTimeoutException if the operation times out.
     */
    @Deprecated
    public static TableResult waitForState(NoSQLHandle handle,
                                           String tableName,
                                           TableResult.State state,
                                           int waitMillis,
                                           int delayMillis) {
        return waitForState(handle,
                            tableName,
                            null,
                            null, /* no operation id */
                            state,
                            waitMillis,
                            delayMillis);
    }

    /**
     * @deprecated use {@link #waitForCompletion} instead.
     *
     * Waits for the specified table to reach the desired state. This is a
     * blocking, polling style wait that delays for the specified number of
     * milliseconds between each polling operation. The state of
     * {@link State#DROPPED} is treated specially in that it will be returned
     * as success, even if the table does not exist. Other states will throw
     * an exception if the table is not found.
     *
     * @param handle the NoSQLHandle to use
     * @param tableName the table name
     * @param operationId optional operation id
     * @param state the desired state
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @return the TableResult representing the table at the desired state
     *
     * @throws IllegalArgumentException if the parameters are not valid.
     *
     * @throws RequestTimeoutException if the operation times out.
     */
    @Deprecated
    public static TableResult waitForState(NoSQLHandle handle,
                                           String tableName,
                                           String operationId,
                                           TableResult.State state,
                                           int waitMillis,
                                           int delayMillis) {

        return waitForState(handle, tableName, null /* compartmentId */,
                            operationId, state, waitMillis, delayMillis);
    }

    /**
     * Waits for the specified table to reach the desired state. This is a
     * blocking, polling style wait that delays for the specified number of
     * milliseconds between each polling operation. The state of {@link
     * State#DROPPED} is treated specially in that it will be returned as
     * success, even if the table does not exist. Other states will throw an
     * exception if the table is not found.
     *
     * @param handle the NoSQLHandle to use
     * @param tableName the table name
     * @param compartment optional compartment name or id if using cloud service
     * @param operationId optional operation id
     * @param state the desired state
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @return the TableResult representing the table at the desired state
     *
     * @throws IllegalArgumentException if the parameters are not valid.
     *
     * @throws RequestTimeoutException if the operation times out.
     */
    private static TableResult waitForState(NoSQLHandle handle,
                                            String tableName,
                                            String compartment,
                                            String operationId,
                                            TableResult.State state,
                                            int waitMillis,
                                            int delayMillis) {
        final int DELAY_MS = 500;

        int delayMS = (delayMillis != 0 ? delayMillis : DELAY_MS);
        if (waitMillis < delayMillis) {
            throw new IllegalArgumentException(
                "Wait milliseconds must be a mininum of " +
                DELAY_MS + " and greater than delay milliseconds");
        }
        long startTime = System.currentTimeMillis();

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(operationId).setCompartment(compartment);
        TableResult res = null;

        do {
            long curTime = System.currentTimeMillis();
            if ((curTime - startTime) > waitMillis) {
                throw new RequestTimeoutException(
                    waitMillis,
                    "Expected table state (" + state + ") for table " +
                    tableName + " not reached ");
            }

            /* delay */
            try {
                if (res != null) {
                    /* only delay after the first getTable */
                    Thread.sleep(delayMS);
                }
                res = handle.getTable(getTable);
            } catch (InterruptedException ie) {
                throw new NoSQLException("waitForState interrupted: " +
                                         ie.getMessage());
            } catch (TableNotFoundException tnf) {
                /* table not found is == DROPPED */
                if (state == State.DROPPED) {
                    return new TableResult().setState(State.DROPPED).
                        setTableName(tableName);
                }
                throw tnf;
            }
        } while (!res.getTableState().equals(state));

        return res;
    }

    /**
     * Waits for a table operation to complete. Table operations are
     * asynchronous. This is a blocking, polling style wait that delays for
     * the specified number of milliseconds between each polling operation.
     * This call returns when the table reaches a <em>terminal</em> state,
     * which is either {@link State#ACTIVE} or {@link State#DROPPED}.
     *
     * This instance must be the return value of a previous
     * {@link NoSQLHandle#tableRequest} and contain a non-null operation id
     * representing the in-progress operation unless the operation has
     * already completed.
     *
     * This instance is modified with any change in table state or metadata.
     *
     * @param handle the NoSQLHandle to use
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @throws IllegalArgumentException if the parameters are not valid.
     *
     * @throws RequestTimeoutException if the operation times out.
     */
    public void waitForCompletion(NoSQLHandle handle,
                                  int waitMillis,
                                  int delayMillis) {

        if (isTerminal()) {
            return;
        }

        if (operationId == null) {
            throw new IllegalArgumentException(
                "Operation state must not be null");
        }

        /* TODO: try to share code with waitForState? */
        final int DELAY_MS = 500;

        int delayMS = (delayMillis != 0 ? delayMillis : DELAY_MS);
        if (waitMillis < delayMillis) {
            throw new IllegalArgumentException(
                "Wait milliseconds must be a mininum of " +
                DELAY_MS + " and greater than delay milliseconds");
        }
        long startTime = System.currentTimeMillis();

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(operationId).setCompartment(domainId);
        TableResult res = null;

        while (!isTerminal()) {

            long curTime = System.currentTimeMillis();
            if ((curTime - startTime) > waitMillis) {
                throw new RequestTimeoutException(
                    waitMillis,
                    "Operation not completed in expected time");
            }

            /* delay */
            try {
                if (res != null) {
                    /* only delay after the first getTable */
                    Thread.sleep(delayMS);
                }
                res = handle.getTable(getTable);
                /*
                 * partial "copy" of possibly modified state. Don't modify
                 * operationId as that is what we are waiting to complete
                 */
                state = res.getTableState();
                limits = res.getTableLimits();
                schema = res.getSchema();
            } catch (InterruptedException ie) {
                throw new NoSQLException("waitForCompletion interrupted: " +
                                         ie.getMessage());
            }
        }
    }

    private boolean isTerminal() {
        return state == State.ACTIVE || state == State.DROPPED;
    }
}
