/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.util.TreeSet;

import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

public class UnionIter extends PlanIter {

    class SortedBranch implements Comparable<SortedBranch> {

        final RuntimeControlBlock theRCB;

        final int theBranchNo;

        final PlanIter theBranch;

        MapValue theCurrentResult;

        boolean theIsDone;

        SortedBranch(RuntimeControlBlock rcb, PlanIter branch, int branchNo) {
            theRCB = rcb;
            theBranchNo = branchNo;
            theBranch = branch;
        }

        @Override
        public int compareTo(SortedBranch other) {

            if (theCurrentResult == null) {
                //theRCB.trace("branch " + theBranchNo + " has no result");
                return -1;
            }

            if (other.theCurrentResult == null) {
                //theRCB.trace("other branch " + other.theBranchNo + " has no result");
                return 1;
            }

            int cmp = Compare.sortResults(theRCB,
                                          theCurrentResult,
                                          other.theCurrentResult,
                                          theSortFields,
                                          theSortSpecs);
            //theRCB.trace("branch " + theBranchNo + " has result " +
            //             theCurrentResult + "\nbranch " + other.theBranchNo +
            //             " has result " + other.theCurrentResult +
            //             "\ncmp = " + cmp);
            if (cmp == 0) {
                return (theBranchNo < other.theBranchNo ? -1 : 1);
            }
            return cmp;
        }

        MapValue next() {

            MapValue res = theCurrentResult;

            boolean more = theBranch.next(theRCB);
            if (more) {
                setCurrentResult();
            } else {
                if (!theRCB.reachedLimit()) {
                    theIsDone = true;
                }
                theCurrentResult = null;
            }

            return res;
        }

        void setCurrentResult() {
            FieldValue val = theRCB.getRegVal(theBranch.getResultReg());
            if (val.isMap()) {
                theCurrentResult = (MapValue)val;
            } else {
                throw new QueryStateException(
                    "Union branch should produce a map value");
            }

            if (theRCB.getTraceLevel() >= 3) {
                theRCB.trace("UNION: branch " + theBranchNo + " received result\n" +
                             theCurrentResult);
            }
        }

        boolean isDone() {
            return theIsDone;
        }
    }

    public static class UnionState extends PlanIterState {

        private int theCurrentBranch;

        private final TreeSet<SortedBranch> theSortedBranches;

        UnionState() {
            theSortedBranches = new TreeSet<SortedBranch>();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCurrentBranch = 0;
            theSortedBranches.clear();
        }
    }

    private final PlanIter[] theBranches;

    private final String[] theSortFields;

    private final SortSpec[] theSortSpecs;

    public UnionIter(ByteInputStream in, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        theBranches = deserializeIters(in, queryVersion);
        theSortFields = SerializationUtil.readStringArray(in);
        theSortSpecs = readSortSpecs(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.UNION;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        UnionState state = new UnionState();
        rcb.setState(theStatePos, state);

        if (theSortFields == null) {
            PlanIter branch = theBranches[state.theCurrentBranch];
            branch.open(rcb);
        } else {
            for (int i = 0; i < theBranches.length; ++i) {
                PlanIter branch = theBranches[i];
                branch.open(rcb);
                SortedBranch sb = new SortedBranch(rcb, branch, i);
                state.theSortedBranches.add(sb);
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        throw new QueryStateException("Unexpected call");
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        UnionState state = (UnionState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter branch : theBranches) {
            branch.close(rcb);
        }

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        if (theSortFields == null) {
            return simpleNext(rcb);
        }
        return sortingNext(rcb);
    }

    private boolean simpleNext(RuntimeControlBlock rcb) {

        UnionState state = (UnionState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (state.theCurrentBranch < theBranches.length) {

            PlanIter branch = theBranches[state.theCurrentBranch];

            boolean more = branch.next(rcb);

            if (more) {
                FieldValue res = rcb.getRegVal(branch.getResultReg());
                rcb.setRegVal(theResultReg, res);
                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("UNION: got result from branch " +
                              state.theCurrentBranch + "\n" + res);
                }
                return true;
            }

            if (rcb.reachedLimit()) {
                return false;
            }

            ++state.theCurrentBranch;
            rcb.incUnionBranch();

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("UNION: moved to branch " +
                          state.theCurrentBranch);
            }

            if (state.theCurrentBranch < theBranches.length) {
                branch = theBranches[state.theCurrentBranch];
                branch.open(rcb);
            }

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("UNION: moved to branch " + state.theCurrentBranch);
            }
        }

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("UNION: is done " + state.theCurrentBranch);
        }
        state.done();
        return false;
    }

    private boolean sortingNext(RuntimeControlBlock rcb) {

        UnionState state = (UnionState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {
            SortedBranch sb = state.theSortedBranches.pollFirst();

            if (sb == null) {
                state.done();
                return false;
            }

            rcb.setUnionBranch(sb.theBranchNo);

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("UNION: requesting result from branch " +
                          sb.theBranchNo);
            }

            MapValue res = sb.next();

            if (res != null) {

                rcb.setRegVal(theResultReg, res);
                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("UNION: got result from branch " +
                              sb.theBranchNo + "\n" + res);
                }

                if (!sb.isDone()) {
                    state.theSortedBranches.add(sb);
                } else {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("UNION : done with branch " + sb.theBranchNo);
                    }
                }

                return true;
            }

            if (!sb.isDone()) {
                state.theSortedBranches.add(sb);
            }

            if (rcb.reachedLimit()) {
                return false;
            }
        }
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("\"branches\" : [\n");
        formatter.incIndent();
        for (int i = 0; i < theBranches.length; ++i) {
            theBranches[i].display(sb, formatter);
            if (i < theBranches.length - 1) {
                sb.append(",\n");
            } else {
               sb.append("\n");
           }
        }
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("]");

        if (theSortFields != null) {

            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"order by fields\" : [ ");
            for (int i = 0; i < theSortFields.length; ++i) {
                sb.append(theSortFields[i]);
                if (i < theSortFields.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");

            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"sort specs\" : [ ");
            for (int i = 0; i < theSortSpecs.length; ++i) {
                sb.append("{ \"desc\" : ").append(theSortSpecs[i].theIsDesc);
                sb.append(", \"nulls_first\" : ").append(theSortSpecs[i].theNullsFirst);
                sb.append(" }");
                if (i < theSortSpecs.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");
        } else {
            sb.append("\n");
        }
    }
}
