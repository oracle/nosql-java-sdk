/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

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

}
