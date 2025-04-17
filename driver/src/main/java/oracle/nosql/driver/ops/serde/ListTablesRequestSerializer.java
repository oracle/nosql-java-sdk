/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class ListTablesRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        ListTablesRequest listRq = (ListTablesRequest) request;

        writeOpCode(out, OpCode.LIST_TABLES);
        serializeRequest(listRq, out);
        out.writeInt(listRq.getStartIndex());
        out.writeInt(listRq.getLimit());
        /* new in V2 */
        writeString(out, listRq.getNamespace());
    }

    @Override
    public ListTablesResult deserialize(Request request,
                                        ByteInputStream in,
                                        short serialVersion)
        throws IOException {

        ListTablesResult result = new ListTablesResult();
        int numTables = readInt(in);
        String[] tables = new String[numTables];
        for (int i = 0; i < numTables; i++) {
            tables[i] = readString(in);
        }
        result.setTables(tables);
        result.setLastIndexReturned(readInt(in));
        return result;
    }
}
