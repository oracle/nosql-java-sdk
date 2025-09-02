/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.DefinedTags;
import oracle.nosql.driver.FreeFormTags;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.http.NoSQLHandleAsyncImpl;
import oracle.nosql.driver.ops.TableLimits.CapacityMode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
     * compartment id (ocid) or namespace (if on-prem)
     */
    private String compartmentOrNamespace;
    /* tableOcid is only used for the cloud service */
    private String tableOcid;
    private String tableName;
    private State state;
    private TableLimits limits;
    private String schema;
    private String ddl;
    private String operationId;
    private FreeFormTags freeFormTags;
    private DefinedTags definedTags;
    private String matchETag;

    private boolean isFrozen;
    private boolean isLocalReplicaInitialized;
    private Replica[] replicas;

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
     * Returns the DDL (create table) statement used to create this table if
     * available. If the table has been altered since initial creation the
     * statement is also altered to reflect the current table schema. This
     * value, when non-null, is functionally equivalent to the schema
     * returned by {@link getSchema}. The most reliable way to get the
     * DDL statement is using {@link NoSQLHandle#getTable} on an existing
     * table.
     *
     * @return the create table statement
     *
     * @since 5.4
     */
    public String getDdl() {
        return ddl;
    }

    /**
     * Cloud service only.
     * <p>
     * Returns the OCID of the table. This value will be null if used with
     * the on-premises service.
     *
     * @return the table OCID
     *
     * @since 5.4
     */
    public String getTableId() {
        return tableOcid;
    }

    /**
     * Cloud service only.
     * <p>
     * Returns compartment id of the target table
     *
     * @return the compartment id if set
     *
     * @since 5.4
     */
    public String getCompartmentId() {
        return compartmentOrNamespace;
    }

    /**
     * On-premises service only.
     * <p>
     * Returns the namespace of the table, or null if it is not in a namespace.
     * Note that the tablename is prefixed with the namespace as well if it
     * is in a namespace.
     *
     * @return the namespace id if set
     *
     * @since 5.4
     */
    public String getNamespace() {
        return compartmentOrNamespace;
    }

    /**
     * Returns the table name of the target table. If on-premises and the table
     * is in a namespace the namespace is included as a prefix using the format
     * <em>namespace:tableName</em>
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the JSON-formatted schema of the table if available and null if
     * not
     * @return the schema
     *
     * @since 5.4
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Returns the throughput and capacity limits for the table.
     * Limits from an on-premises service will always be null.
     *
     * @return the limits
     */
    public TableLimits getTableLimits() {
        return limits;
    }

    /**
     * Cloud service only.
     *
     * Returns the {@link FreeFormTags} associated with this table,
     * if available, or null otherwise.
     *
     * @return the FreeFormTags
     *
     * @since 5.4
     */
    public FreeFormTags getFreeFormTags() {
        return freeFormTags;
    }

    /**
     * Cloud service only.
     *
     * Returns the {@link DefinedTags} associated with this table,
     * if available, or null otherwise.
     *
     * @return the DefinedTags
     *
     * @since 5.4
     */
    public DefinedTags getDefinedTags() {
        return definedTags;
    }

    /**
     * Cloud service only.
     *
     * Returns the matchETag associated with this table. The matchETag is an
     * opaque field that represents the current version of the table itself and
     * can be used in future table modification operations to only perform
     * them if the matchETag for the table has not changed. This is an
     * optimistic concurrency control mechanism.
     *
     * @return the matchETag
     *
     * @since 5.4
     */
    public String getMatchETag() {
        return matchETag;
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
     * Returns whether or not the table's schema is frozen. The schema
     * can only be frozen when using the cloud service and a frozen
     * schema is required for replicating a table
     *
     * @return true if the schema is frozen
     *
     * @since 5.4.13
     */
    public boolean isFrozen() {
        return isFrozen;
    }

    /**
     * Cloud service only.
     *
     * Returns true if the table is replicated.
     *
     * @return the flag
     *
     * @since 5.4.13
     */
    public boolean isReplicated() {
        return replicas != null;
    }

    /**
     * Cloud service only.
     *
     * Returns true if the table is a replica and its initialization process
     * has been completed, otherwise return false.
     *
     * @return true if the table is a replica and it's been initialized
     *
     * @since 5.4.13
     */
    public boolean isLocalReplicaInitialized() {
        return isLocalReplicaInitialized;
    }

    /**
     * Cloud service only.
     *
     * Returns a array of {@link Replica} if the table is replicated,
     * or null if table is not replicated.
     *
     * @return the remote Replicas
     *
     * @since 5.4.13
     */
    public Replica[] getReplicas() {
        return replicas;
    }

    /**
     * internal use only
     * @param operationId the operation id
     * @return this
     * @hidden
     */
    public TableResult setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    /**
     * internal use only
     * @param state the table state
     * @return this
     * @hidden
     */
    public TableResult setState(State state) {
        this.state = state;
        return this;
    }

    /**
     * internal use only
     * @param value the compartment OCID
     * @return this
     * @hidden
     */
    public TableResult setCompartmentId(String value) {
        this.compartmentOrNamespace = value;
        return this;
    }

    /**
     * internal use only
     * @param value the namespace
     * @return this
     * @hidden
     */
    public TableResult setNamespace(String value) {
        this.compartmentOrNamespace = value;
        return this;
    }

    /**
     * internal use only
     * @param value the OCID
     * @return this
     * @hidden
     */
    public TableResult setTableId(String value) {
        this.tableOcid = value;
        return this;
    }

    /**
     * internal use only
     * @param tableName the table name
     * @return this
     * @hidden
     */
    public TableResult setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * internal use only
     * @param schema the schema
     * @return this
     * @hidden
     */
    public TableResult setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    /**
     * internal use only
     * @param ddl the ddl
     * @return this
     * @hidden
     */
    public TableResult setDdl(String ddl) {
        this.ddl = ddl;
        return this;
    }

    /**
     * internal use only
     * @param limits table limits
     * @return this
     * @hidden
     */
    public TableResult setTableLimits(TableLimits limits) {
        this.limits = limits;
        return this;
    }

    /**
     * internal use only
     * @param tags the tags
     * @return this
     * @since 5.4
     * @hidden
     */
    public TableResult setFreeFormTags(FreeFormTags tags) {
        this.freeFormTags = tags;
        return this;
    }

    /**
     * internal use only
     * @param tags the tags
     * @return this
     * @since 5.4
     * @hidden
     */
    public TableResult setDefinedTags(DefinedTags tags) {
        this.definedTags = tags;
        return this;
    }

    /**
     * internal use only
     * @param matchETag the matchETag
     * @return this
     * @since 5.4
     * @hidden
     */
    public TableResult setMatchETag(String matchETag) {
        this.matchETag = matchETag;
        return this;
    }

    /**
     * internal use only
     * @param frozen true if frozen
     * @return this
     * @since 5.4.13
     * @hidden
     */
    public TableResult setIsFrozen(boolean frozen) {
        this.isFrozen = frozen;
        return this;
    }

    /**
     * internal use only
     * @param initialized the flag
     * @return this
     * @since 5.4.13
     * @hidden
     */
    public TableResult setLocalReplicaInitialized(boolean initialized) {
        this.isLocalReplicaInitialized = initialized;
        return this;
    }

    /**
     * internal use only
     * @param replicas the Replica array
     * @return this
     * @since 5.4.13
     * @hidden
     */
    public TableResult setReplicas(Replica[] replicas) {
        this.replicas = replicas;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table ");
        sb.append(tableName).append("state=[").append(state).append("] ");
        if (limits != null) {
            sb.append("\nlimits=").append(limits);
        }
        if (schema != null) {
            sb.append("\nschema=[" + schema + "]");
        }
        if (ddl != null) {
            sb.append("\nddl=[" + ddl + "]");
        }
        if (operationId != null) {
            sb.append("\noperationId=").append(operationId);
        }
        if (matchETag != null) {
            sb.append("\nmatchETag=").append(matchETag);
        }
        if (isFrozen) {
            sb.append("\nisFrozen=").append(isFrozen);
        }
        if (isReplicated()) {
            sb.append("\nisLocalReplicaInitialized=")
              .append(isLocalReplicaInitialized);
            sb.append("\nreplicas=[");
            boolean first = true;
            for (Replica rep : replicas) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(rep.toString());
            }
            sb.append("]");
        }
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
     * @param waitMillis the total amount of time to wait, in milliseconds. This
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
     * @param waitMillis the total amount of time to wait, in milliseconds. This
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
     * @param waitMillis the total amount of time to wait, in milliseconds. This
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
     * @param waitMillis the total amount of time to wait, in milliseconds. This
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
                "Wait milliseconds must be a minimum of " +
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
     * @param waitMillis the total amount of time to wait, in milliseconds. This
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
                "Wait milliseconds must be a minimum of " +
                DELAY_MS + " and greater than delay milliseconds");
        }
        long startTime = System.currentTimeMillis();

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(operationId).setCompartment(
                compartmentOrNamespace);
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
                matchETag = res.getMatchETag();
                ddl = res.getDdl();
                isFrozen = res.isFrozen();
                isLocalReplicaInitialized = res.isLocalReplicaInitialized();
                replicas = res.getReplicas();
            } catch (InterruptedException ie) {
                throw new NoSQLException("waitForCompletion interrupted: " +
                                         ie.getMessage());
            }
        }
    }

    public CompletableFuture<Void> waitForCompletionAsync
            (NoSQLHandleAsyncImpl handle, int waitMillis, int delayMillis) {

        if (isTerminal()) {
            return CompletableFuture.completedFuture(null);
        }

        if (operationId == null) {
            Throwable t = new IllegalArgumentException(
                "Operation state must not be null");
            return CompletableFuture.failedFuture(t);
        }

        /* TODO: try to share code with waitForState? */
        final int DELAY_MS = 500;

        final int delayMS = (delayMillis != 0 ? delayMillis : DELAY_MS);
        if (waitMillis < delayMillis) {
            Throwable t = new IllegalArgumentException(
                "Wait milliseconds must be a minimum of " +
                DELAY_MS + " and greater than delay milliseconds");
            return CompletableFuture.failedFuture(t);
        }

        final long startTime = System.currentTimeMillis();
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        final ScheduledExecutorService taskExecutor = handle.getTaskExecutor();

        GetTableRequest getTable =
            new GetTableRequest().setTableName(tableName).
            setOperationId(operationId).setCompartment(
                compartmentOrNamespace);

        Runnable poll = new Runnable() {
            @Override
            public void run() {
                long curTime = System.currentTimeMillis();
                if ((curTime - startTime) > waitMillis) {
                    Throwable t = new RequestTimeoutException(
                        waitMillis,
                        "Operation not completed in expected time");
                    resultFuture.completeExceptionally(t);
                    return;
                }
                handle.getTable(getTable).whenComplete((res, ex) -> {
                    if (ex != null) {
                        resultFuture.completeExceptionally(ex);
                        return;
                    }
                    /*
                     * partial "copy" of possibly modified state. Don't modify
                     * operationId as that is what we are waiting to complete
                     */
                    state = res.getTableState();
                    limits = res.getTableLimits();
                    schema = res.getSchema();
                    matchETag = res.getMatchETag();
                    ddl = res.getDdl();
                    isFrozen = res.isFrozen();
                    isLocalReplicaInitialized = res.isLocalReplicaInitialized();
                    replicas = res.getReplicas();

                    if (isTerminal()) {
                        resultFuture.complete(null);
                    } else {
                        /* Schedule next poll */
                        taskExecutor.schedule(this, delayMS,
                            TimeUnit.MILLISECONDS);
                    }
                });
            }
        };
        /* Kick off the first poll immediately */
        taskExecutor.execute(poll);
        return resultFuture;
    }

    private boolean isTerminal() {
        return state == State.ACTIVE || state == State.DROPPED;
    }

    /**
     * Cloud service only.
     *
     * Information representing a single remote replica
     *
     * @since 5.4.13
     */
    public static class Replica {
        private String replicaName;
        private String tableOcid;
        private int writeUnits;
        private CapacityMode mode;
        private State state;

        /**
         * @hidden
         */
        public Replica() {
        }

        /**
         * Returns the name of the replica. This is the region name.
         *
         * @return the replica name
         */
        public String getReplicaName() {
            return replicaName;
        }

        /**
         * Returns the OCID of the replica table
         *
         * @return the table OCID
         */
        public String getTableId() {
            return tableOcid;
        }

        /**
         * Returns the write units of the replica table
         *
         * @return the write units
         */
        public int getWriteUnits() {
            return writeUnits;
        }


        /**
         * Returns the capacity mode of the replica table
         *
         * @return the capacity mode
         */
        public CapacityMode getCapacityMode() {
            return mode;
        }

        /**
         * Returns the operational state of replica table
         *
         * @return the state
         */
        public State getState() {
            return state;
        }

        /**
         * internal use only
         * @param replicaName the replica name
         * @return this
         * @hidden
         */
        public Replica setReplicaName(String replicaName) {
            this.replicaName = replicaName;
            return this;
        }

        /**
         * internal use only
         * @param tableOcid the OCID of remote replica table
         * @return this
         * @hidden
         */
        public Replica setTableId(String tableOcid) {
            this.tableOcid = tableOcid;
            return this;
        }

        /**
         * internal use only
         * @param writeUnits the write units of remote replica table
         * @return this
         * @hidden
         */
        public Replica setWriteUnits(int writeUnits) {
            this.writeUnits = writeUnits;
            return this;
        }

        /**
         * internal use only
         * @param mode the capacity mode of remote replica table
         * @return this
         * @hidden
         */
        public Replica setCapacityMode(CapacityMode mode) {
            this.mode = mode;
            return this;
        }

        /**
         * internal use only
         * @param state the state of remote replication table
         * @return this
         * @hidden
         */
        public Replica setState(State state) {
            this.state = state;
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Replica[replicaName=").append(replicaName);
            sb.append(", state=").append(state);
            if (tableOcid != null) {
                sb.append(", tableId=").append(tableOcid);
            }
            if (writeUnits > 0) {
                sb.append(", writeUnits=").append(writeUnits);
            }
            if (mode != null) {
                sb.append(", mode=").append(mode);
            }
            sb.append("]");
            return sb.toString();
        }
    }
}
