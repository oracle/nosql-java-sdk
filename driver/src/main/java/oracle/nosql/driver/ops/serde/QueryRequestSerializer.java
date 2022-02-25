/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;


/**
 * Queries
 */
class QueryRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        QueryRequest queryRq = (QueryRequest) request;

        /* write unconditional state first */
        writeOpCode(out, OpCode.QUERY);
        serializeRequest(queryRq, out);
        writeConsistency(out, queryRq.getConsistency());
        writeInt(out, queryRq.getLimit());
        writeInt(out, queryRq.getMaxReadKB());
        writeByteArray(out, queryRq.getContKey());
        out.writeBoolean(queryRq.isPrepared());

        /* the following 7 fields were added in V2 */
        out.writeShort(QueryDriver.QUERY_VERSION);
        out.writeByte((byte)queryRq.getTraceLevel());
        writeInt(out, queryRq.getMaxWriteKB());
        SerializationUtil.writeMathContext(queryRq.getMathContext(), out);
        writeInt(out, queryRq.topologySeqNum());
        writeInt(out, queryRq.getShardId());
        out.writeBoolean(queryRq.isPrepared() && queryRq.isSimpleQuery());

        if (queryRq.isPrepared()) {

            PreparedStatement ps = queryRq.getPreparedStatement();
            writeByteArrayWithInt(out, ps.getStatement());

            if (ps.getVariables() != null) {

                Map<String, FieldValue> vars = ps.getVariables();
                writeInt(out, vars.size());

                for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
                    writeString(out, entry.getKey());
                    writeFieldValue(out, entry.getValue());
                }
            } else {
                writeInt(out, 0);
            }
        } else {
            writeString(out, queryRq.getStatement());
        }
    }

    @Override
    public QueryResult deserialize(
         Request request,
         ByteInputStream in,
         short serialVersion) throws IOException {

        QueryRequest qreq = (QueryRequest) request;
        PreparedStatement prep = qreq.getPreparedStatement();
        boolean isPrepared = (prep != null);

        QueryResult result = new QueryResult(qreq);

        int numRows = in.readInt();
        boolean isSortPhase1Result = in.readBoolean();

        List<MapValue> results = new ArrayList<MapValue>(numRows);
        result.setResults(results);
        for (int i = 0; i < numRows; i++) {
            MapValue val = readFieldValue(in).asMap();
            results.add(val);
        }

        if (isSortPhase1Result) {
            result.setIsInPhase1(in.readBoolean());
            int[] pids = readIntArray(in);
            if (pids != null) {
                result.setPids(pids);
                result.setNumResultsPerPid(readIntArray(in));
                byte[][] contKeys = new byte[pids.length][];
                for (int i = 0; i < pids.length; ++i) {
                    contKeys[i] = readByteArray(in);
                }
                result.setPartitionContKeys(contKeys);
            }
        }

        deserializeConsumedCapacity(in, result);
        result.setContinuationKey(readByteArray(in));
        qreq.setContKey(result.getContinuationKey());

        /*
         * In V2, if the QueryRequest was not initially prepared, the prepared
         * statement created at the proxy is returned back along with the query
         * results, so that the preparation does not need to be done during each
         * query batch.
         */
        if (!isPrepared) {

            prep = PrepareRequestSerializer.
                   deserializeInternal(qreq.getStatement(),
                                       false,
                                       in,
                                       serialVersion);

            qreq.setPreparedStatement(prep);
        }

        if (prep != null && !prep.isSimpleQuery()) {
            if (!isPrepared) {
                assert(numRows == 0);
                QueryDriver driver = new QueryDriver(qreq);
                driver.setTopologyInfo(prep.topologyInfo());
                driver.setPrepCost(result.getReadKB());
                result.setComputed(false);
            } else {
                /* In this case, the QueryRequest is an "internal" one */
                result.setReachedLimit(in.readBoolean());
                TopologyInfo ti = BinaryProtocol.readTopologyInfo(in);
                QueryDriver driver = qreq.getDriver();
                if (ti != null) {
                    prep.setTopologyInfo(ti);
                    driver.setTopologyInfo(ti);
                }
            }
        }

        return result;
    }
}
