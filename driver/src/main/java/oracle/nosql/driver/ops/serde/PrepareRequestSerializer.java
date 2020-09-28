/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import static oracle.nosql.driver.http.Client.trace;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.query.PlanIter;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

/**
 * Prepare a query
 */
class PrepareRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        PrepareRequest prepRq = (PrepareRequest) request;

        writeOpCode(out, OpCode.PREPARE);
        serializeRequest(prepRq, out);
        writeString(out, prepRq.getStatement());
        out.writeShort(QueryDriver.QUERY_VERSION);
        out.writeBoolean(prepRq.getQueryPlan());
    }

    @Override
    public PrepareResult deserialize(Request request,
                                     ByteInputStream in,
                                     short serialVersion)
        throws IOException {

        PrepareRequest prepRq = (PrepareRequest) request;
        PrepareResult result = new PrepareResult();
        deserializeConsumedCapacity(in, result);

        PreparedStatement prepStmt =
            deserializeInternal(prepRq.getStatement(),
                                prepRq.getQueryPlan(),
                                in,
                                serialVersion);

        result.setPreparedStatement(prepStmt);
        return result;
    }

    static PreparedStatement deserializeInternal(
        String sqlText,
        boolean getQueryPlan,
        ByteInputStream in,
        short serialVersion)  throws IOException {

        byte[] proxyStatement = readByteArrayWithInt(in);

        int numIterators = 0;
        int numRegisters = 0;
        Map<String, Integer> externalVars = null;
        TopologyInfo ti = null;
        String queryPlan = null;

        if (getQueryPlan) {
            queryPlan = readString(in);
        }

        PlanIter driverPlan = PlanIter.deserializeIter(in, serialVersion);

        if (driverPlan != null) {
            numIterators = in.readInt();
            numRegisters = in.readInt();

            trace("PREP-RESULT: Query Plan:\n" + driverPlan.display() + "\n", 1);

            int len = in.readInt();
            if (len > 0) {
                externalVars = new HashMap<String, Integer>(len);
                for (int i = 0; i < len; ++i) {
                    String varName = readString(in);
                    int varId = in.readInt();
                    externalVars.put(varName, varId);
                }
            }

            ti = BinaryProtocol.readTopologyInfo(in);
        }

        return new PreparedStatement(sqlText,
                                     queryPlan,
                                     ti,
                                     proxyStatement,
                                     driverPlan,
                                     numIterators,
                                     numRegisters,
                                     externalVars);
    }
}
