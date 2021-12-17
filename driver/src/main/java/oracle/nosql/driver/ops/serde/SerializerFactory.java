/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

import java.io.IOException;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

/**
 * @hidden
 */
public interface SerializerFactory {

    /* serializers */
    Serializer createDeleteSerializer();

    Serializer createGetSerializer();

    Serializer createPutSerializer();

    Serializer createQuerySerializer();

    Serializer createPrepareSerializer();

    Serializer createGetTableSerializer();

    Serializer createGetTableUsageSerializer();

    Serializer createListTablesSerializer();

    Serializer createGetIndexesSerializer();

    Serializer createTableOpSerializer();

    Serializer createSystemOpSerializer();

    Serializer createSystemStatusSerializer();

    Serializer createWriteMultipleSerializer();

    Serializer createMultiDeleteSerializer();

    /* deserializers */
    Serializer createDeleteDeserializer();

    Serializer createGetDeserializer();

    Serializer createPutDeserializer();

    Serializer createQueryDeserializer();

    Serializer createPrepareDeserializer();

    Serializer createGetTableDeserializer();

    Serializer createGetTableUsageDeserializer();

    Serializer createListTablesDeserializer();

    Serializer createGetIndexesDeserializer();

    Serializer createTableOpDeserializer();

    Serializer createSystemOpDeserializer();

    Serializer createSystemStatusDeserializer();

    Serializer createWriteMultipleDeserializer();

    Serializer createMultiDeleteDeserializer();

    /*
     * These methods encapsulate differences in serializer streams
     */
    default int readErrorCode(ByteInputStream bis) throws IOException {
        return 0;
    }

    default void writeSerialVersion(short serialVersion, ByteOutputStream bis)
        throws IOException {
    }

    default String getSerdeVersionString() {
        return null;
    }
}
