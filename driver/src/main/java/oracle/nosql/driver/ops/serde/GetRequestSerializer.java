/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import static oracle.nosql.driver.util.BinaryProtocol.V2;

import java.io.IOException;

import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class GetRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        GetRequest getRq = (GetRequest) request;

        writeOpCode(out, OpCode.GET);
        serializeReadRequest(getRq, out);
        writeFieldValue(out, getRq.getKey());
    }

    @Override
    public GetResult deserialize(Request request,
                                 ByteInputStream in,
                                 short serialVersion)
        throws IOException {

        GetResult result = new GetResult();
        deserializeConsumedCapacity(in, result);
        boolean hasRow = in.readBoolean();
        if (hasRow) {
            result.setValue(readFieldValue(in).asMap());
            result.setExpirationTime(readLong(in));
            result.setVersion(readVersion(in));
            if (serialVersion > V2) {
                result.setModificationTime(readLong(in));
            } else {
                result.setModificationTime(0);
            }
        }
        return result;
    }
}
