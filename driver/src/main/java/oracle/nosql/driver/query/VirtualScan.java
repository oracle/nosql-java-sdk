/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;


public class VirtualScan {

    public static class TableResumeInfo {

        final private int theCurrentIndexRange;
        final private byte[] thePrimResumeKey;        
        final private byte[] theSecResumeKey;
        final private boolean theMoveAfterResumeKey;

        final private byte[] theDescResumeKey;
        final private int[] theJoinPathTables;
        final private byte[] theJoinPathKey;
        final private byte[] theJoinPathSecKey;
        final private boolean theJoinPathMatched;

        public TableResumeInfo(
            int currentIndexRange,
            byte[] primKey,
            byte[] secKey,
            boolean moveAfterResumeKey,
            byte[] descResumeKey,
            int[] joinPathTables,
            byte[] joinPathKey,
            byte[] joinPathSecKey,
            boolean joinPathMatched) {

            theCurrentIndexRange = currentIndexRange;
            thePrimResumeKey = primKey;
            theSecResumeKey = secKey;
            theMoveAfterResumeKey = moveAfterResumeKey;
            theDescResumeKey = descResumeKey;
            theJoinPathTables = joinPathTables;
            theJoinPathKey = joinPathKey;
            theJoinPathSecKey = joinPathSecKey;
            theJoinPathMatched = joinPathMatched;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder();

            sb.append("theCurrentIndexRange = ").append(theCurrentIndexRange);
            sb.append("\n");

            if (thePrimResumeKey != null) {
                sb.append("thePrimResumeKey = ");
                sb.append(PlanIter.printByteArray(thePrimResumeKey));
                sb.append("\n");
            }

            if (theSecResumeKey != null) {
                sb.append("theSecResumeKey = ");
                sb.append(PlanIter.printByteArray(theSecResumeKey));
                sb.append("\n");
            }

            sb.append("theMoveAfterResumeKey = ").append(theMoveAfterResumeKey);
            sb.append("\n");

            if (theDescResumeKey != null) {
                sb.append("theDescResumeKey = ");
                sb.append(PlanIter.printByteArray(theDescResumeKey));
                sb.append("\n");
            }

            if (theJoinPathTables != null) {
                sb.append("theJoinPathTables = ");
                sb.append(PlanIter.printIntArray(theJoinPathTables));
                sb.append("\n");
            }

            if (theJoinPathKey != null) {
                sb.append("theJoinPathKey = ");
                sb.append(PlanIter.printByteArray(theJoinPathKey));
                sb.append("\n");
            }

            if (theJoinPathSecKey != null) {
                sb.append("theJoinPathSecKey = ");
                sb.append(PlanIter.printByteArray(theJoinPathSecKey));
                sb.append("\n");
            }

            sb.append("theJoinPathMatched = ").append(theJoinPathMatched);
            sb.append("\n");

            return sb.toString();
        }
    }

    final private int theSID;
    final private int thePID;
    final private TableResumeInfo[] theTableRIs;


    boolean theFirstBatch = true;

    public VirtualScan(
        int pid,
        int sid,
        TableResumeInfo[] tableRIs) {

        theSID = sid;
        thePID = pid;
        theTableRIs = tableRIs;
    }

    public int sid() {
        return theSID;
    }

    public int pid() {
        return thePID;
    }

    public int numTables() {
        return theTableRIs.length;
    }

    public int currentIndexRange(int i) {
        return theTableRIs[i].theCurrentIndexRange;
    }

    public byte[] secKey(int i) {
        return theTableRIs[i].theSecResumeKey;
    }

    public byte[] primKey(int i) {
        return theTableRIs[i].thePrimResumeKey;
    }
    
    public boolean moveAfterResumeKey(int i) {
        return theTableRIs[i].theMoveAfterResumeKey;
    }

    public byte[] descResumeKey(int i) {
        return theTableRIs[i].theDescResumeKey;
    }

    public int[] joinPathTables(int i) {
        return theTableRIs[i].theJoinPathTables;
    }

    public byte[] joinPathKey(int i) {
        return theTableRIs[i].theJoinPathKey;
    }

    public byte[] joinPathSecKey(int i) {
        return theTableRIs[i].theJoinPathSecKey;
    }

    public boolean joinPathMatched(int i) {
        return theTableRIs[i].theJoinPathMatched;
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

        for (int i = 0; i < theTableRIs.length; ++i) {
            sb.append("Table RI ").append(i).append(":\n");
            sb.append(theTableRIs[i]);
        }

        return sb.toString();
    }
}
