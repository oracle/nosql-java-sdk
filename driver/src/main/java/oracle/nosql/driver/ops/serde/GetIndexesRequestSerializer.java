/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetIndexesResult.IndexInfo;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class GetIndexesRequestSerializer extends BinaryProtocol
    implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        GetIndexesRequest getRq = (GetIndexesRequest) request;

        writeOpCode(out, OpCode.GET_INDEXES);
        serializeRequest(getRq, out);
        writeString(out, getRq.getTableName());
        if (getRq.getIndexName() != null) {
            out.writeBoolean(true);
            writeString(out, getRq.getIndexName());
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public Result deserialize(Request request,
                              ByteInputStream in,
                              short serialVersion)
        throws IOException {

        GetIndexesResult result = new GetIndexesResult();
        int numIndexes = readInt(in);
        IndexInfo[] indexes = new IndexInfo[numIndexes];
        for (int i = 0; i < numIndexes; i++) {
            indexes[i] = deserializeIndexInfo(in);
        }
        result.setIndexes(indexes);
        return result;
    }

    private IndexInfo deserializeIndexInfo(ByteInputStream in)
        throws IOException {

        String indexName = readString(in);
        int nFields = readInt(in);
        String[] fieldNames = new String[nFields];
        for (int n = 0; n < nFields; n++) {
            fieldNames[n] = readString(in);
        }
        return new IndexInfo(indexName, fieldNames);
    }
}
