/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.util.Arrays;

public class TopologyInfo {

    private int theSeqNum = -1;

    private int[] theShardIds;

    public TopologyInfo(int seqNum, int[] shardIds) {
        theSeqNum = seqNum;
        theShardIds = shardIds;
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

    int[] getShardIds() {
        return theShardIds;
    }

    @Override
    public boolean equals(Object o) {

        TopologyInfo other = (TopologyInfo)o;

        if (this == other ||
            theSeqNum == other.theSeqNum ||
            Arrays.equals(theShardIds, other.theShardIds)) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return theSeqNum;
    }
}
