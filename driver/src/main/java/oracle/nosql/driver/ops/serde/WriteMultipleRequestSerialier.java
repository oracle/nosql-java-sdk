/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class WriteMultipleRequestSerializer extends BinaryProtocol
    implements Serializer {

    private final Serializer putSerializer;
    private final Serializer deleteSerializer;

    WriteMultipleRequestSerializer(Serializer putSerializer,
                                   Serializer deleteSerializer) {
        this.putSerializer = putSerializer;
        this.deleteSerializer = deleteSerializer;
    }

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        WriteMultipleRequest umRq = (WriteMultipleRequest) request;
        int num = umRq.getNumOperations();

        /* OpCode */
        writeOpCode(out, OpCode.WRITE_MULTIPLE);
        serializeRequest(umRq, out);
        /* TableName */
        writeString(out, umRq.getTableName());

        /* The number of operations */
        writeInt(out, num);

        /* Operations */
        for (OperationRequest op : umRq.getOperations()) {
            int start = out.getOffset();

            /* AbortIfUnsuccessful flag */
            out.writeBoolean(op.isAbortIfUnsuccessful());
            WriteRequest req = op.getRequest();
            req.setCheckRequestSize(umRq.getCheckRequestSize());
            if (req instanceof PutRequest) {
                putSerializer.serialize(req, serialVersion, out);
            } else {
                assert(req instanceof DeleteRequest) :
                      "Not an instance of DeleteRequest: " +
                      req.getClass().getName();
                deleteSerializer.serialize(req, serialVersion, out);
            }

            /* Check each sub request size limit */
            checkRequestSizeLimit(req, (out.getOffset() - start));
        }
    }

    @Override
    public WriteMultipleResult deserialize(Request request,
                                           ByteInputStream in,
                                           short serialVersion)
        throws IOException {

        final WriteMultipleResult umResult = new WriteMultipleResult();
        boolean succeed = in.readBoolean();
        deserializeConsumedCapacity(in, umResult);
        if (succeed) {
            int num = readInt(in);
            for (int i = 0; i < num; i++) {
                umResult.addResult(createOperationResult(in));
            }
        } else {
            umResult.setFailedOperationIndex(in.readByte());
            umResult.addResult(createOperationResult(in));
        }
        return umResult;
    }

    private OperationResult createOperationResult(ByteInputStream in)
        throws IOException {

        OperationResult opResult = new OperationResult();

        /* Success flag */
        boolean success = in.readBoolean();
        opResult.setSuccess(success);

        /* New version */
        boolean hasVersion = in.readBoolean();
        if (hasVersion) {
            Version version = readVersion(in);
            opResult.setVersion(version);
        }

        /* Previous value and version */
        deserializeWriteResponse(in, opResult);

        /* Generated value, if present */
        boolean hasGeneratedValue = in.readBoolean();
        if (hasGeneratedValue) {
            opResult.setGeneratedValue(readFieldValue(in));
        }
        return opResult;
    }
}
