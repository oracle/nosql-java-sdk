/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

/**
 * @hidden
 * An interface for serialization/deserialization of Requests and their Results
 */
public interface Serializer {

    /*
     * The interface to serialize a request
     */
    void serialize(Request request,
                   short serialVersion,
                   ByteOutputStream out) throws IOException;

    /*
     * Deserialize a response
     */
    Result deserialize(Request request,
                       ByteInputStream in,
                       short serialVersion) throws IOException;
}
