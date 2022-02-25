/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops.serde;

/**
 * @hidden
 */
public class BinarySerializerFactory implements SerializerFactory {

    static final DeleteRequestSerializer delSerializer =
        new DeleteRequestSerializer();
    static final GetRequestSerializer getSerializer =
        new GetRequestSerializer();
    static final PutRequestSerializer putSerializer =
        new PutRequestSerializer();

    static final QueryRequestSerializer querySerializer =
        new QueryRequestSerializer();
    static final PrepareRequestSerializer prepareSerializer =
        new PrepareRequestSerializer();
    static final GetTableRequestSerializer getTableSerializer =
        new GetTableRequestSerializer();
    static final TableUsageRequestSerializer getTableUsageSerializer =
        new TableUsageRequestSerializer();
    static final TableRequestSerializer tableSerializer =
        new TableRequestSerializer();
    static final SystemRequestSerializer systemSerializer =
        new SystemRequestSerializer();
    static final SystemStatusRequestSerializer systemStatusSerializer =
        new SystemStatusRequestSerializer();
    static final ListTablesRequestSerializer listTablesSerializer =
        new ListTablesRequestSerializer();
    static final GetIndexesRequestSerializer getIndexesSerializer =
        new GetIndexesRequestSerializer();

    static final WriteMultipleRequestSerializer writeMultipleSerializer =
        new WriteMultipleRequestSerializer(new PutRequestSerializer(true),
                                           new DeleteRequestSerializer(true));
    static final MultiDeleteRequestSerializer multiDeleteSerializer =
        new MultiDeleteRequestSerializer();

    @Override
    public Serializer createDeleteSerializer() {
        return delSerializer;
    }

    @Override
    public Serializer createGetSerializer() {
        return getSerializer;
    }

    @Override
    public Serializer createPutSerializer() {
        return putSerializer;
    }

    @Override
    public Serializer createQuerySerializer() {
        return querySerializer;
    }

    @Override
    public Serializer createPrepareSerializer() {
        return prepareSerializer;
    }

    @Override
    public Serializer createGetTableSerializer() {
        return getTableSerializer;
    }

    @Override
    public Serializer createGetTableUsageSerializer() {
        return getTableUsageSerializer;
    }

    @Override
    public Serializer createListTablesSerializer() {
        return listTablesSerializer;
    }

    @Override
    public Serializer createGetIndexesSerializer() {
        return getIndexesSerializer;
    }

    @Override
    public Serializer createTableOpSerializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createSystemOpSerializer() {
        return systemSerializer;
    }

    @Override
    public Serializer createSystemStatusSerializer() {
        return systemStatusSerializer;
    }

    @Override
    public Serializer createWriteMultipleSerializer() {
        return writeMultipleSerializer;
    }

    @Override
    public Serializer createMultiDeleteSerializer() {
        return multiDeleteSerializer;
    }

    /* deserializers */
    @Override
    public Serializer createDeleteDeserializer() {
        return delSerializer;
    }

    @Override
    public Serializer createGetDeserializer() {
        return getSerializer;
    }

    @Override
    public Serializer createPutDeserializer() {
        return putSerializer;
    }

    @Override
    public Serializer createQueryDeserializer() {
        return querySerializer;
    }

    @Override
    public Serializer createPrepareDeserializer() {
        return prepareSerializer;
    }

    @Override
    public Serializer createGetTableDeserializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createGetTableUsageDeserializer() {
        return getTableUsageSerializer;
    }

    @Override
    public Serializer createListTablesDeserializer() {
        return listTablesSerializer;
    }

    @Override
    public Serializer createGetIndexesDeserializer() {
        return getIndexesSerializer;
    }

    @Override
    public Serializer createTableOpDeserializer() {
        return tableSerializer;
    }

    @Override
    public Serializer createSystemOpDeserializer() {
        return systemSerializer;
    }

    @Override
    public Serializer createSystemStatusDeserializer() {
        return systemStatusSerializer;
    }

    @Override
    public Serializer createWriteMultipleDeserializer() {
        return writeMultipleSerializer;
    }

    @Override
    public Serializer createMultiDeleteDeserializer() {
        return multiDeleteSerializer;
    }
}
