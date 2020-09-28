/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class SystemRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        SystemRequest rq = (SystemRequest) request;

        writeOpCode(out, OpCode.SYSTEM_REQUEST);
        serializeRequest(rq, out);
        writeCharArrayAsUTF8(out, rq.getStatement());
    }

    @Override
    public SystemResult deserialize(Request request,
                                 ByteInputStream in,
                                 short serialVersion)
        throws IOException {

        return deserializeSystemResult(in);
    }
}
