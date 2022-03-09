/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.RequestTimeoutException;

/**
 * On-premise only.
 * <p>
 * SystemResult is returned from {@link NoSQLHandle#systemStatus} and
 * {@link NoSQLHandle#systemRequest} operations. It encapsulates the
 * state of the operation requested.
 * <p>
 * Some operations performed by {@link NoSQLHandle#systemRequest} are asynchronous.
 * When such an operation has been performed it is necessary to call
 * {@link NoSQLHandle#systemStatus} until the status of the operation is known.
 * The method {@link #waitForCompletion} exists to perform this task and should
 * be used whenever possible.
 * <p>
 * Asynchronous operations (e.g. create namespace) can be distinguished from
 * synchronous System operations in this way:
 * <ul>
 * <li>Asynchronous operations may return a non-null operationId</li>
 * <li>Asynchronous operations modify state, while synchronous operations
 * are read-only</li>
 * <li>Synchronous operations return a state of {@link State#COMPLETE} and have a
 * non-null resultString</li>
 * </ul>
 *
 * {@link NoSQLHandle#systemStatus} is synchronous, returning the known state of
 * the operation. It should only be called if the operation was asynchronous
 * and returned a non-null operationId.
 *
 * @see NoSQLHandle#systemStatus
 * @see NoSQLHandle#systemRequest
 */
public class SystemResult extends Result {
    private State state;
    private String operationId;
    private String statement;
    private String resultString;

    /**
     * On-premise only.
     * <p>
     * The current state of the operation
     */
    public enum State {
        /**
         * The operation is complete and was successful. Failures are
         * thrown as exceptions.
         */
        COMPLETE,
        /**
         * The operation is in progress
         */
        WORKING
    }

    /**
     * Returns the operation state.
     *
     * @return the state
     */
    public State getOperationState() {
        return state;
    }

    /**
     * Returns the operation id for the operation if it was asynchronous.
     * This is null if the request did not generate a new operation and/or
     * the operation state is {@link State#COMPLETE}. The
     * value can be used in {@link SystemStatusRequest#setOperationId} to
     * get status and find potential errors resulting from the operation.
     *
     * This method is only useful for the result of asynchronous operations.
     * @return the operation id or null if not set
     */
    public String getOperationId() {
        return operationId;
    }

    /**
     * Returns the result string for the operation. This is null if
     * the request was asynchronous or did not return an actual result.
     * For example the "show" operations return a non-null result string,
     * but "create, drop, grant, etc" operations return a null result
     * string.
     * @return the string
     */
    public String getResultString() {
        return resultString;
    }

    /**
     * Returns the statement used for the operation.
     *
     * @return the statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * @hidden
     * Sets the statement to use for the operation. This parameter is
     * required.
     *
     * @param statement the statement
     *
     * @return this
     */
    public SystemResult setStatement(String statement) {
        this.statement = statement;
        return this;
    }

    /**
     * @hidden
     * @param operationId the operation id
     * @return this
     */
    public SystemResult setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    /**
     * @hidden
     * @param resultString the result string
     * @return this
     */
    public SystemResult setResultString(String resultString) {
        this.resultString = resultString;
        return this;
    }

    /**
     * @hidden
     * @param state the state
     * @return this
     */
    public SystemResult setState(State state) {
        this.state = state;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SystemResult [statement=").append(statement)
            .append(", state=").append(state)
            .append(", operationId=").append(operationId)
            .append(", resultString=").append(resultString).append("]");
        return sb.toString();
    }

    /**
     * Waits for the operation to be complete. This is a
     * blocking, polling style wait that delays for the specified number of
     * milliseconds between each polling operation.
     *
     * This instance is modified with any changes in state.
     *
     * @param handle the NoSQLHandle to use
     * @param waitMillis the total amount of time to wait, in millseconds. This
     * value must be non-zero and greater than delayMillis
     * @param delayMillis the amount of time to wait between polling attempts, in
     * milliseconds. If 0 it will default to 500.
     *
     * @throws IllegalArgumentException if the operation times out or the
     * parameters are not valid.
     *
     * @throws NoSQLException if the operation id used is unknown or
     * the operation has failed.
     */
    public void waitForCompletion(NoSQLHandle handle,
                                  int waitMillis,
                                  int delayMillis) {
        if (state.equals(State.COMPLETE)) {
            return;
        }

        final int DELAY_MS = 500;

        int delayMS = (delayMillis != 0 ? delayMillis : DELAY_MS);
        if (waitMillis < delayMillis) {
            throw new IllegalArgumentException(
                "Wait milliseconds must be a mininum of " +
                DELAY_MS + " and greater than delay milliseconds");
        }
        long startTime = System.currentTimeMillis();

        SystemStatusRequest ds = new SystemStatusRequest()
            .setOperationId(operationId);
        SystemResult res = null;

        do {
            long curTime = System.currentTimeMillis();
            if ((curTime - startTime) > waitMillis) {
                throw new RequestTimeoutException(
                    waitMillis,
                    "Operation not completed within timeout: " +
                    statement);
            }

            /* delay */
            try {
                if (res != null) {
                    /* only delay after the first getTable */
                    Thread.sleep(delayMS);
                }
                res = handle.systemStatus(ds);
                /*
                 * do partial copy of new state.
                 * statement and operationId are not changed.
                 */
                resultString = res.resultString;
                state = res.state;
            } catch (InterruptedException ie) {
                throw new NoSQLException("waitForCompletion interrupted: " +
                                         ie.getMessage());
            }
        } while (!state.equals(State.COMPLETE));
    }
}
