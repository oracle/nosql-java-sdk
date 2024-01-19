/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.TableUsageResult.TableUsage;
import oracle.nosql.driver.util.BinaryProtocol.OpCode;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

class TableUsageRequestSerializer extends BinaryProtocol implements Serializer {

    @Override
    public void serialize(Request request,
                          short serialVersion,
                          ByteOutputStream out)
        throws IOException {

        TableUsageRequest getRq = (TableUsageRequest) request;

        writeOpCode(out, OpCode.GET_TABLE_USAGE);
        serializeRequest(getRq, out);
        writeString(out, getRq.getTableName());
        writeLong(out, getRq.getStartTime());
        writeLong(out, getRq.getEndTime());
        writeInt(out, getRq.getLimit());
    }

    @Override
    public TableUsageResult deserialize(Request request,
                                        ByteInputStream in,
                                        short serialVersion)
        throws IOException {

        TableUsageResult result = new TableUsageResult();
        /* don't use tenantId, but it's in the result */
        readString(in); // tenantId
        result.setTableName(readString(in));
        int numResults = readInt(in);
        TableUsage[] usageRecords = new TableUsage[numResults];
        for (int i = 0; i < numResults; i++) {
            usageRecords[i] = deserializeUsage(in);
        }
        result.setUsageRecords(usageRecords);
        return result;
    }

    private TableUsage deserializeUsage(ByteInputStream in)
        throws IOException {

        TableUsage usage = new TableUsage();
        usage.startTimeMillis = readLong(in);
        usage.secondsInPeriod = readInt(in);
        usage.readUnits = readInt(in);
        usage.writeUnits = readInt(in);
        usage.storageGB = readInt(in);
        usage.readThrottleCount = readInt(in);
        usage.writeThrottleCount = readInt(in);
        usage.storageThrottleCount = readInt(in);
        return usage;
    }
}
