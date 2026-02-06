/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class DeleteRequestSerializer extends BinaryProtocol implements Serializer {

    /*
     * The flag indicates if the serializer is used for a standalone request
     * or a sub operation of WriteMultiple request.
     *
     * If it is used to serialize the sub operation, then some information
     * like SerialVersion, Timeout, Namespace and TableName will be skipped
     * during serialization.
     */
    private final boolean isSubRequest;

    DeleteRequestSerializer() {
        this(false);
    }

    DeleteRequestSerializer(boolean isSubRequest) {
        this.isSubRequest = isSubRequest;
    }

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        DeleteRequest deleteRq = (DeleteRequest) request;
        Version matchVersion = deleteRq.getMatchVersion();

        writeOpCode(out, (matchVersion != null ? OpCode.DELETE_IF_VERSION :
                          OpCode.DELETE));
        if (isSubRequest) {
            out.writeBoolean(deleteRq.getReturnRow());
        } else {
            serializeWriteRequest(deleteRq, out, serialVersion);
        }
        writeFieldValue(out, deleteRq.getKey());
        if (matchVersion != null) {
            writeVersion(out, matchVersion);
        }
    }

    @Override
    public DeleteResult deserialize(Request request,
                                    ByteInputStream in,
                                    short serialVersion)
        throws IOException {

        DeleteResult result = new DeleteResult();
        deserializeConsumedCapacity(in, result);
        boolean success = in.readBoolean();
        result.setSuccess(success);
        deserializeWriteResponse(in, result, serialVersion);
        return result;
    }
}
