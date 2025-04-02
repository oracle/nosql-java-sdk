/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import static oracle.nosql.driver.http.Client.trace;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.UnsupportedQueryVersionException;
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
public class PrepareRequestSerializer extends BinaryProtocol
                                      implements Serializer {
    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {
            throw new IllegalArgumentException("Missing query version " +
                      "in prepare request serializer");
    }

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          short queryVersion,
                          ByteOutputStream out)
        throws IOException {

        /* QUERY_V4 and above not supported by V3 protocol */
        if (queryVersion >= QueryDriver.QUERY_V4) {
            throw new UnsupportedQueryVersionException(
                "Query version " + queryVersion +
                " not supported by V3 protocol");
        }

        PrepareRequest prepRq = (PrepareRequest) request;

        writeOpCode(out, OpCode.PREPARE);
        serializeRequest(prepRq, out);
        writeString(out, prepRq.getStatement());
        out.writeShort(queryVersion);
        out.writeBoolean(prepRq.getQueryPlan());
    }

    @Override
    public PrepareResult deserialize(
         Request request,
         ByteInputStream in,
         short serialVersion) throws IOException {
            throw new IllegalArgumentException("Missing query version " +
                      "in prepare request deserializer");
    }

    @Override
    public PrepareResult deserialize(
         Request request,
         ByteInputStream in,
         short serialVersion,
         short queryVersion) throws IOException {

        /* QUERY_V4 and above not supported by V3 protocol */
        if (queryVersion >= QueryDriver.QUERY_V4) {
            throw new UnsupportedQueryVersionException(
                "Query version " + queryVersion +
                " not supported by V3 protocol");
        }

        PrepareRequest prepRq = (PrepareRequest) request;
        PrepareResult result = new PrepareResult();
        deserializeConsumedCapacity(in, result);

        deserializeInternal(prepRq.getStatement(),
                            prepRq.getQueryPlan(),
                            result,
                            in,
                            serialVersion);
        return result;
    }

    static PreparedStatement deserializeInternal(
        String sqlText,
        boolean getQueryPlan,
        PrepareResult result,
        ByteInputStream in,
        short serialVersion)  throws IOException {

        /*
         * Extract the table name and namespace from the prepared query.
         * This dips into the portion of the prepared query that is
         * normally opaque
         *
         * int (4 byte)
         * byte[] (32 bytes -- hash)
         * byte (number of tables)
         * namespace (string)
         * tablename (string)
         * operation (1 byte)
         */
        int savedOffset = in.getOffset();
        in.skip(37); // 4 + 32 + 1
        String namespace = readString(in);
        String tableName = readString(in);
        byte operation = in.readByte();
        in.setOffset(savedOffset);

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

        PreparedStatement prep =
            new PreparedStatement(sqlText,
                                  queryPlan,
                                  null, // query schema
                                  proxyStatement,
                                  driverPlan,
                                  numIterators,
                                  numRegisters,
                                  externalVars,
                                  namespace,
                                  tableName,
                                  operation,
                                  0); /* no parallelism available */

        result.setPreparedStatement(prep);
        result.setTopology(ti);

        return prep;
    }
}
