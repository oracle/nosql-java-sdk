/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;


public class VirtualScan {

    final private int theSID;

    final private int thePID;

    boolean theFirstBatch = true;

    public VirtualScan(int pid, int sid) {
        theSID = sid;
        thePID = pid;
    }

    public int sid() {
        return theSID;
    }

    public int pid() {
        return thePID;
    }

    public boolean isFirstBatch() {
        return theFirstBatch;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("sid/pid = ").append(theSID).append("/").append(thePID);
        sb.append("\n");

        sb.append("theFirstBatch = ").append(theFirstBatch);
        sb.append("\n");
        return sb.toString();
    }
}
