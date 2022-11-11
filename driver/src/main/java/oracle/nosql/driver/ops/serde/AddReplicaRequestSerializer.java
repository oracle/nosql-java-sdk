/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;

class AddReplicaRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        AddReplicaRequest req = (AddReplicaRequest) request;

        writeOpCode(out, OpCode.ADD_REPLICA);
        serializeRequest(req, out);
        writeString(out, req.getTableName());
        writeString(out, req.getRegion());
        writeInt(out, req.getReadUnits());
        writeInt(out, req.getWriteUnits());
    }

    @Override
    public Result deserialize(Request request,
                              ByteInputStream in,
                              short serialVersion)
        throws IOException {

        return deserializeTableResult(in, serialVersion);
    }
}
