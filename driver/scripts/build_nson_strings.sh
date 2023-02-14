#!/usr/bin/env bash

# Copyright (C) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
#
# This file was distributed by Oracle as part of a version of Oracle NoSQL
# Database made available at:
#
# http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
#
# Please see the LICENSE file included in the top-level directory of the
# appropriate version of Oracle NoSQL Database for a copy of the license and
# additional information.

# this script creates java code to enable using human-readable strings for field
# names in nson debug/logging/verbose output. It modifies NsonProtocol.java to
# add a map of field names to human readable field names.

PROTOCOL_FILE=src/main/java/oracle/nosql/driver/ops/serde/nson/NsonProtocol.java

# delete everything after the last static string
lastline=$(grep -n 'public static String [A-Z]' $PROTOCOL_FILE | tail -1 | sed -e 's/:.*$//')
head -$lastline $PROTOCOL_FILE > /tmp/proto.$$

# add a static array of string arrays
grep 'public static String ' /tmp/proto.$$ | awk 'BEGIN{printf("\n    private static String[][] mapVals = new String[][] {\n");}{printf("        {%s,\"%s\"},\n",$4,$4);}END{printf("    };\n");}' >> /tmp/proto.$$

# add remaining logic
cat << EOT >> /tmp/proto.$$

    private static HashMap<String, String> fieldMap = null;

    public static String readable(String field) {
        if (fieldMap == null) {
            fieldMap = new HashMap<String, String>();
            for (int x=0; x<mapVals.length; x++) {
                fieldMap.put(mapVals[x][0], mapVals[x][1]);
            }
        }
        String val = fieldMap.get(field);
        if (val == null) {
            return field;
        }
        return val;
    }
}
EOT

# replace file
mv /tmp/proto.$$ $PROTOCOL_FILE

