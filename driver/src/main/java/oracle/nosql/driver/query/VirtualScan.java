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

    public VirtualScan(
        int pid,
        int sid,
        byte[] primKey,
        byte[] secKey,
        boolean moveAfterResumeKey) {
        theSID = sid;
        thePID = pid;
        thePrimResumeKey = primKey;
        theSecResumeKey = secKey;
        theMoveAfterResumeKey = moveAfterResumeKey;
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

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("sid/pid = ").append(theSID).append("/").append(thePID);
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

        return sb.toString();
    }
}
