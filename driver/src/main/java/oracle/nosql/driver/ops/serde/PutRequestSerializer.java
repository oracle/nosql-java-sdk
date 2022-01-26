/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class PutRequestSerializer extends BinaryProtocol implements Serializer {

    /*
     * The flag indicates if the serializer is used for a standalone request
     * or a sub operation of WriteMultiple request.
     *
     * If it is used to serialize the sub operation, then some information
     * like SerialVersion, Timeout, Namespace and TableName will be skipped
     * during serialization.
     */
    private final boolean isSubRequest;

    PutRequestSerializer() {
        this(false);
    }

    PutRequestSerializer(boolean isSubRequest) {
        this.isSubRequest = isSubRequest;
    }

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        PutRequest putRq = (PutRequest) request;

        OpCode op = getOpCode(putRq);
        writeOpCode(out, op);
        if (isSubRequest) {
            out.writeBoolean(putRq.getReturnRow());
        } else {
            serializeWriteRequest(putRq, out);
        }
        out.writeBoolean(putRq.getExactMatch());
        writeInt(out, putRq.getIdentityCacheSize());
        writeFieldValue(out, putRq.getValue());
        out.writeBoolean(putRq.getUpdateTTL());
        writeTTL(out, putRq.getTTL());
        if (putRq.getMatchVersion() != null) {
            writeVersion(out, putRq.getMatchVersion());
        }
    }

    @Override
    public PutResult deserialize(Request request,
                                 ByteInputStream in,
                                 short serialVersion)
        throws IOException {

        PutResult result = new PutResult();
        deserializeConsumedCapacity(in, result);
        boolean success = in.readBoolean();

        if (success) {
            result.setVersion(readVersion(in));
        }

        /* return row info */
        deserializeWriteResponse(in, result);

        /* generated identity column value */
        deserializeGeneratedValue(in, result);

        return result;
    }

    /**
     * Assumes that the request has been validated and only one of the
     * if options is set, if any.
     */
    private OpCode getOpCode(PutRequest req) {
        if (req.getOption() == null) {
            return OpCode.PUT;
        }
        switch (req.getOption()) {
        case IfAbsent:
            return OpCode.PUT_IF_ABSENT;
        case IfPresent:
            return OpCode.PUT_IF_PRESENT;
        case IfVersion:
            return OpCode.PUT_IF_VERSION;
        default:
            throw new IllegalStateException("Unknown Options " +
                req.getOption());
        }
    }
}
