/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class MultiDeleteRequestSerializer extends BinaryProtocol
    implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        MultiDeleteRequest mdRq = (MultiDeleteRequest) request;
        writeOpCode(out, OpCode.MULTI_DELETE);
        serializeRequest(mdRq, out);
        writeString(out, mdRq.getTableName());
        writeDurability(out, mdRq.getDurability(), serialVersion);
        writeFieldValue(out, mdRq.getKey());
        writeFieldRange(out, mdRq.getRange());
        writeInt(out, mdRq.getMaxWriteKB());
        writeByteArray(out, mdRq.getContinuationKey());
    }

    @Override
    public MultiDeleteResult deserialize(Request request,
                                         ByteInputStream in,
                                         short serialVersion)
        throws IOException {

        MultiDeleteResult result = new MultiDeleteResult();
        deserializeConsumedCapacity(in, result);
        result.setNumDeletions(readInt(in));
        result.setContinuationKey(readByteArray(in));
        return result;
    }
}
