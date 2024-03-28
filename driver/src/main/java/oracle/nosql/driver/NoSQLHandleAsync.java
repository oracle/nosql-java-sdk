package oracle.nosql.driver;

import oracle.nosql.driver.ops.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public interface NoSQLHandleAsync extends AutoCloseable {
    Publisher<GetResult> get(GetRequest request);

    Publisher<PutResult> put(PutRequest request);

    Publisher<DeleteResult> delete(DeleteRequest request);
    Publisher<TableResult> getTable(GetTableRequest request);

    Publisher<TableResult> tableRequest(TableRequest request);

    Publisher<ListTablesResult> listTables(ListTablesRequest request);

    Publisher<QueryResult> query(QueryRequest request);
}
