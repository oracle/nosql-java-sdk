/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class TableRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        TableRequest rq = (TableRequest) request;

        writeOpCode(out, OpCode.TABLE_REQUEST);
        serializeRequest(rq, out);
        writeString(out, rq.getStatement());

        TableLimits limits = rq.getTableLimits();
        if (limits != null) {
            out.writeBoolean(true);
            // TODO create serializer for limits
            out.writeInt(limits.getReadUnits());
            out.writeInt(limits.getWriteUnits());
            out.writeInt(limits.getStorageGB());
            writeLimitsMode(out, limits.getMode());
            if (rq.getTableName() != null) {
                /* table name may exist with limits */
                out.writeBoolean(true);
                writeString(out, rq.getTableName());
            } else {
                out.writeBoolean(false);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public TableResult deserialize(Request request,
                                   ByteInputStream in,
                                   short serialVersion)
        throws IOException {

        return deserializeTableResult(in);
    }
}
