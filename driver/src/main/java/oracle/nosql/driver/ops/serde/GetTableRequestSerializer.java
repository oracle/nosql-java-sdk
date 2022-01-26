/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class GetTableRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        GetTableRequest getRq = (GetTableRequest) request;

        writeOpCode(out, OpCode.GET_TABLE);
        serializeRequest(getRq, out);
        writeString(out, getRq.getTableName());
        writeString(out, getRq.getOperationId());
    }

    @Override
    public TableResult deserialize(Request request,
                                   ByteInputStream in,
                                   short serialVersion)
        throws IOException {

        return deserializeTableResult(in);
    }

}
