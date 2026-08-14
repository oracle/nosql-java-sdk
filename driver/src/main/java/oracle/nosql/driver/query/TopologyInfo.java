/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.util.Arrays;
import java.util.Objects;

public class TopologyInfo {

    private final String theStoreName;

    private final int theSeqNum;

    private final int[] theShardIds;

    public TopologyInfo(int seqNum, int[] shardIds) {
        this(null, seqNum, shardIds);
    }

    public TopologyInfo(String storeName, int seqNum, int[] shardIds) {
        if (storeName != null && storeName.isBlank()) {
            throw new IllegalArgumentException(
                "TopologyInfo storeName must not be empty string");
        }
        if (seqNum < 0) {
            throw new IllegalArgumentException(
                "TopologyInfo seqNum must not be negative");
        }
        if (shardIds == null || shardIds.length == 0) {
            throw new IllegalArgumentException(
                "TopologyInfo shardIds must not be null or empty");
        }
        theStoreName = storeName;
        theSeqNum = seqNum;
        theShardIds = shardIds;
    }

    public String getStoreName() {
        return theStoreName;
    }

    public int getSeqNum() {
        return theSeqNum;
    }

    int numShards() {
        return theShardIds.length;
    }

    int getShardId(int i) {
        return theShardIds[i];
    }

    int getLastShardId() {
        return theShardIds[theShardIds.length-1];
    }

    @Override
    public boolean equals(Object o) {

        if (!(o instanceof TopologyInfo)) {
            return false;
        }
        TopologyInfo other = (TopologyInfo)o;

        if (this == other ||
            (Objects.equals(theStoreName, other.theStoreName) &&
             theSeqNum == other.theSeqNum &&
             Arrays.equals(theShardIds, other.theShardIds))) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        int code = 1;
        code = 31 * code + (theStoreName != null ? theStoreName.hashCode() : 0);
        code = 31 * code + theSeqNum;
        code = 31 * code + Arrays.hashCode(theShardIds);
        return code;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        if (theStoreName != null) {
            sb.append("storeName = ").append(theStoreName).append(" ");
        }
        sb.append("seqNum = ").append(theSeqNum);
        sb.append(" shards ids = [ ");
        for (int sid : theShardIds) {
            sb.append(sid).append(" ");
        }
        sb.append("]");

        return sb.toString();
    }
}
