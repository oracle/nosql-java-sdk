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

    final private byte[] thePrimResumeKey;        
    final private byte[] theSecResumeKey;
    final private boolean theMoveAfterResumeKey;

    final private byte[] theDescResumeKey;
    final private int[] theJoinPathTables;
    final private byte[] theJoinPathKey;
    final private byte[] theJoinPathSecKey;
    final private boolean theJoinPathMatched;

    boolean theFirstBatch = true;

    public VirtualScan(
        int pid,
        int sid,
        byte[] primKey,
        byte[] secKey,
        boolean moveAfterResumeKey,
        byte[] descResumeKey,
        int[] joinPathTables,
        byte[] joinPathKey,
        byte[] joinPathSecKey,
        boolean joinPathMatched) {
        theSID = sid;
        thePID = pid;
        thePrimResumeKey = primKey;
        theSecResumeKey = secKey;
        theMoveAfterResumeKey = moveAfterResumeKey;
        theDescResumeKey = descResumeKey;
        theJoinPathTables = joinPathTables;
        theJoinPathKey = joinPathKey;
        theJoinPathSecKey = joinPathSecKey;
        theJoinPathMatched = joinPathMatched;
    }

    public int sid() {
        return theSID;
    }

    public int pid() {
        return thePID;
    }

    public byte[] secKey() {
        return theSecResumeKey;
    }

    public byte[] primKey() {
        return thePrimResumeKey;
    }

    public boolean moveAfterResumeKey() {
        return theMoveAfterResumeKey;
    }

    public byte[] descResumeKey() {
        return theDescResumeKey;
    }

    public int[] joinPathTables() {
        return theJoinPathTables;
    }

    public byte[] joinPathKey() {
        return theJoinPathKey;
    }

    public byte[] joinPathSecKey() {
        return theJoinPathSecKey;
    }

    public boolean joinPathMatched() {
        return theJoinPathMatched;
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
