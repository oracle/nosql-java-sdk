/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

        /*
         * TableName
         * If all ops use the same table name, write that
         * single table name to the output stream.
         * If any of them are different, write all table
         * names, comma-separated.
         */
        if (umRq.isSingleTable()) {
            writeString(out, umRq.getTableName());
        } else {
            StringBuilder sb = new StringBuilder();
            for (OperationRequest op : umRq.getOperations()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(op.getRequest().getTableName());
            }
            writeString(out, sb.toString());
        }

        /* The number of operations */
        writeInt(out, num);

        /* Durability setting */
        writeDurability(out, umRq.getDurability(), serialVersion);

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
                umResult.addResult(createOperationResult(in, serialVersion));
            }
        } else {
            umResult.setFailedOperationIndex(in.readByte());
            umResult.addResult(createOperationResult(in, serialVersion));
        }
        return umResult;
    }

    private OperationResult createOperationResult(ByteInputStream in,
                                                  short serialVersion)
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
        deserializeWriteResponse(in, opResult, serialVersion);

        /* Generated value, if present */
        boolean hasGeneratedValue = in.readBoolean();
        if (hasGeneratedValue) {
            opResult.setGeneratedValue(readFieldValue(in));
        }
        return opResult;
    }
}
