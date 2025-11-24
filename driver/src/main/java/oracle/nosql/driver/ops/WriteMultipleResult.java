/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the result of a {@link NoSQLHandle#writeMultiple} operation.
 *
 * <p>
 * If the WriteMultiple succeeds, the execution result of each sub operation
 * can be retrieved using {@link #getResults}.
 * <p>
 * If the WriteMultiple operation is aborted because of the failure of an
 * operation with abortIfUnsuccessful set to true, then the index of failed
 * operation can be accessed using {@link #getFailedOperationIndex}, and the
 * execution result of failed operation can be accessed using
 * {@link #getFailedOperationResult}.
 * @see NoSQLHandle#writeMultiple
 */
public class WriteMultipleResult extends Result {

    private final List<OperationResult> results;

    private int failedOperationIndex;

    /**
     * @hidden
     */
    public WriteMultipleResult() {
        results = new ArrayList<OperationResult>();
        failedOperationIndex = -1;
    }

    /**
     * Returns true if the WriteMultiple operation succeeded, or false if
     * the operation is aborted due to the failure of a sub operation.
     * <P>
     * The failed operation index can be accessed using
     * {@link #getFailedOperationIndex} and its result can be accessed using
     * {@link #getFailedOperationResult}.
     *
     * @return true if the operation succeeded
     */
    public boolean getSuccess() {
        return (failedOperationIndex == -1);
    }

    /**
     * Returns the list of execution results for the operations.
     *
     * @return the list of execution results
     */
    public List<OperationResult> getResults() {
        return results;
    }

    /**
     * Returns the number of results.
     *
     * @return the number of results
     */
    public int size() {
        return results.size();
    }

    /**
     * Returns the index of failed operation that results in the entire
     * WriteMultiple operation aborting.
     *
     * @return the index of operation, -1 if not set.
     */
    public int getFailedOperationIndex() {
        return failedOperationIndex;
    }

    /**
     * Returns the result of the operation that results in the entire
     * WriteMultiple operation aborting.
     *
     * @return the result of the operation, null if not set.
     */
    public OperationResult getFailedOperationResult() {
        if (failedOperationIndex == -1 || results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number because this was an update operation.
     *
     * @return the read KBytes consumed
     */
    public int getReadKB() {
        return super.getReadKBInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in KBytes.
     *
     * @return the write KBytes consumed
     */
    public int getWriteKB() {
        return super.getWriteKBInternal();
    }

    /**
     * Returns the read throughput consumed by this operation, in read units.
     * This number may be larger than that returned by {@link #getReadKB}
     * because it was an update operation.
     *
     * @return the read units consumed
     */
    public int getReadUnits() {
        return super.getReadUnitsInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in write
     * units.
     *
     * @return the write units consumed
     */
    public int getWriteUnits() {
        return super.getWriteUnitsInternal();
    }

    @Override
    public String toString() {
        if (getSuccess()) {
            return ("WriteMultiple, num results: " + results.size());
        }
        return ("WriteMultiple aborted, the failed operation index: " +
                failedOperationIndex);
    }

    /**
     * @hidden
     * @param result the result
     * @return this
     */
    public WriteMultipleResult addResult(OperationResult result) {
        results.add(result);
        return this;
    }

    /**
     * @hidden
     * @param index the index of a failed operation
     * @return this
     */
    public WriteMultipleResult setFailedOperationIndex(int index) {
        failedOperationIndex = index;
        return this;
    }

    /**
     * The Result associated with the execution of an individual
     * operation in the request.
     */
    public static class OperationResult extends WriteResult {

        private boolean success;
        private Version version;
        private FieldValue generatedValue;

        /**
         * Returns the flag indicates whether the operation succeeded.  A put
         * or delete operation may be unsuccessful if the condition is not
         * matched.
         * @return true if the operation succeeded
         */
        public boolean getSuccess() {
            return success;
        }

        /**
         * Returns the version of the new row for put operation, or null
         * if put operations did not succeed or the operation is delete
         * operation.
         * @return the version if it exists
         */
        public Version getVersion() {
            return version;
        }

        /**
         * Returns the existing row version associated with the key if
         * available.
         * @return the existing version if set
         */
        public Version getExistingVersion() {
            return super.getExistingVersionInternal();
        }

        /**
         * Returns the existing row value associated with the key if available.
         * @return the existing value if set
         */
        public MapValue getExistingValue() {
            return super.getExistingValueInternal();
        }

        /**
         * Returns the creation time associated with the key if
         * available.
         * Note: If the row was written by a version of the system older than
         * 25.3 the creation time will be equal to the modification time, if it
         * was written by a system older than 19.5 it will be zero.
         *
         * @return the creation time if set, in milliseconds sine Jan 1, 1970
         * GMT
         *
         * @since 5.4.18
         */
        public long getExistingCreationTime() {
            return super.getExistingCreationTimeInternal();
        }

        /**
         * Returns the existing modification time associated with the key if
         * available.
         * @return the modification time if set, in milliseconds sine Jan 1,
         * 1970 GMT
         *
         * @since 5.3.0
         */
        public long getExistingModificationTime() {
            return super.getExistingModificationTimeInternal();
        }

        /**
         * Returns the value generated if the operation created a new value.
         * This can happen if the table contains an identity column or string
         * column declared as a generated UUID. If the table has no such
         * columns this value is null. If a value was generated for the
         * operation, it is non-null.
         *
         * This value is only valid for a put operation on a table with
         * an identity column or string as uuid column.
         *
         * @return the generated value
         */
        public FieldValue getGeneratedValue() {
            return generatedValue;
        }

        @Override
        public String toString() {
            return "Success: " + success + ", version: " + version +
                ", existing version: " + getExistingVersion() +
                ", existing value: " +
                (getExistingValue() == null ?
                 "null" : getExistingValue().toJson()) +
                ", generated value: " +
                (getGeneratedValue() == null ?
                 "null" : getGeneratedValue().toJson());
        }

        /**
         * @hidden
         * @param success success
         * @return this
         */
        public OperationResult setSuccess(boolean success) {
            this.success = success;
            return this;
        }

        /**
         * @hidden
         * @param version version
         * @return this
         */
        public OperationResult setVersion(Version version) {
            this.version = version;
            return this;
        }

        /**
         * @hidden
         * @param value value
         * @return this
         */
        public OperationResult setGeneratedValue(FieldValue value) {
            this.generatedValue = value;
            return this;
        }
    }
}
