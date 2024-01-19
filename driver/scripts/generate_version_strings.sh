#!/usr/bin/env bash

# Copyright (C) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

SDK_VERSION_FILE=src/main/java/oracle/nosql/driver/SDKVersion.java

# get the version string from pom file
sdk_version=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
if [ "$sdk_version" = "" ] ; then
  echo "Error getting version from pom file"
  exit 1
fi

# delete everything after the package declaration
lastline=$(grep -n 'package oracle.nosql.driver' $SDK_VERSION_FILE | tail -1 | sed -e 's/:.*$//')
head -$lastline $SDK_VERSION_FILE > /tmp/sdkversion.$$

# add SDKVersion class 
cat << EOT >> /tmp/sdkversion.$$
/**
 * Public class to manage SDK version information
 */
public class SDKVersion {
    /**
     * The full X.Y.Z version of the current SDK
     */
    public static final String VERSION = "$sdk_version";
}
EOT

# replace file
mv /tmp/sdkversion.$$ $SDK_VERSION_FILE
echo "Set SDKVersion.VERSION to $sdk_version"
exit 0

