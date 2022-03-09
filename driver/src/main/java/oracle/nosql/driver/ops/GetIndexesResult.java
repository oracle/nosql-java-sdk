/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the result of a {@link NoSQLHandle#getIndexes} operation.
 * <p>
 * On a successful operation the index information is returned in an
 * array of {@link IndexInfo}
 *
 * @see NoSQLHandle#getIndexes
 */
public class GetIndexesResult extends Result {

    private IndexInfo[] indexes;

    /**
     * Returns the array of index information returned by the operation.
     *
     * @return the indexes information
     */
    public IndexInfo[] getIndexes() {
        return indexes;
    }

    /**
     * @hidden
     * @param indexes the indexes
     * @return this
     */
    public GetIndexesResult setIndexes(IndexInfo[] indexes) {
        this.indexes = indexes;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < indexes.length; i++) {
            indexes[i].toStringBuilder(sb);
            if (i < (indexes.length - 1)) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * IndexInfo represents the information about a single index including
     * its name and field names.
     */
    public static class IndexInfo {

        private String indexName;
        private String[] fieldNames;

        /**
         * @hidden
         * @param indexName the index
         * @param fieldNames the fields
         */
        public IndexInfo(String indexName, String[] fieldNames) {
            this.indexName = indexName;
            this.fieldNames = fieldNames;
        }

        /**
         * Returns the name of the index.
         *
         * @return the index name
         */
        public String getIndexName() {
            return indexName;
        }

        /**
         * Returns the array of field names that define the index.
         *
         * @return the field names
         */
        public String[] getFieldNames() {
            return fieldNames;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toStringBuilder(sb);
            return sb.toString();
        }

        private void toStringBuilder(StringBuilder sb) {
            sb.append("IndexInfo [indexName=").append(indexName);
            sb.append(", fields=[");
            for (int i = 0; i < fieldNames.length; i++) {
                sb.append(fieldNames[i]);
                if (i < fieldNames.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]]");
        }
    }
}
