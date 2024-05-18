package oracle.nosql.driver;

import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static oracle.nosql.driver.NoSQLHandleFactory.createNoSQLHandleAsync;

public class AsyncTest {
    static void testAsync() throws InterruptedException {
        final String endpoint = "http://localhost:8080";
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint, new AuthorizationProvider() {
            @Override
            public String getAuthorizationString(Request request) {
                return "Bearer id";
            }

            @Override
            public Publisher<String> getAuthorizationStringAsync(Request request) {
                return Mono.just("Bearer id");
            }

            @Override
            public void close() {

            }
        });


        Hooks.onErrorDropped(Throwable::printStackTrace);
        NoSQLHandleAsync asyncHandle = createNoSQLHandleAsync(config);
        final String tableName = "sample";
        final String tableDDL = "CREATE TABLE IF NOT EXISTS " + tableName +
                "(ID LONG, DOC JSON, PRIMARY KEY(ID))";

        TableRequest tableRequest = new TableRequest()
                .setStatement(tableDDL)
                .setTableLimits(new TableLimits(50,50,1));

        // create table and wait for it to complete
        System.out.println("Creating table " + tableName);
                Mono.from(asyncHandle.doTableRequest(tableRequest,
                                Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .doOnNext(result -> System.out.println("Created table " + tableName))
                .doOnError(throwable -> System.out.println("Table creation " +
                        "failed for " + tableName +":" + throwable.getMessage()))
                .block();

        // put a row
        System.out.println("Putting a row");
        MapValue value = new MapValue();
        value.put("id", 1);
        value.putFromJson("doc", "{\"name\" : \"abc\",\"age\" : 23}", null);
        PutRequest putRequest =
                new PutRequest().setValue(value).setTableName(tableName);

        Mono.from(asyncHandle.put(putRequest))
                .subscribe(putResult -> System.out.println("Put success"),
                error -> System.out.println("Put failed :" + error.getMessage()));

        // get row
        System.out.println("Get a row");
        GetRequest getRequest = new GetRequest().setKey(new MapValue().put(
                "id",1)).setTableName(tableName);
        Mono.from(asyncHandle.get(getRequest)).subscribe(
                getResult -> System.out.println("Got " + "row: " + getResult.getValue()),
                error -> System.out.println("Get row failed: " + error.getMessage())

        );

        // insert a row using query
        System.out.println("Inserting a row using query");
        String insertQuery = "insert into " + tableName + " values(2, " +
                "{\"name\" : " +
                "\"pinto\", \"age\": 45})";
        QueryRequest queryRequest =
                new QueryRequest().setStatement(insertQuery);
        Mono.from(asyncHandle.query(queryRequest)).subscribe(result ->
                System.out.println("Insert success"),
                error -> System.out.println("Insert failure. Error: " + error.getMessage()));

        // query table
        String query = "SELECT * from " + tableName;
        QueryRequest queryRequest1 =
                new QueryRequest().setStatement(query).setMaxReadKB(500);
        Publisher<QueryResult> queryPublisher = asyncHandle.query(queryRequest1);

        Flux.from(queryPublisher).subscribe(new BaseSubscriber<QueryResult>() {
            Subscription subscription;
            AtomicInteger count = new AtomicInteger();
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                this.subscription = subscription;
                System.out.println("[subscriber ] requesting 10 pages");
                subscription.request(10);
            }

            @Override
            protected void hookOnNext(QueryResult value) {
                System.out.println("[subscriber] Received total of " + value.getResults().size() + " records");
                count.addAndGet(value.getResults().size());
                System.out.println("Consumed " + value.getReadUnits() + " units");
                subscription.request(10);
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("[subscriber] done");
                System.out.println("total read = " + count.get());
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                System.out.println("[subscriber] Error occurred");
                throwable.printStackTrace();
            }
        });


        //sleep for 10 seconds for other operations to complete
        Thread.sleep(10000);

        // drop table
        System.out.println("Dropping the table");
        String dropDDL = "drop table " + tableName;
        Mono.from(asyncHandle.doTableRequest(
                new TableRequest().setStatement(dropDDL),
                Duration.ofSeconds(1),Duration.ofMillis(250)
        )).block();

    }

    public static void main(String[] args) throws InterruptedException {
        testAsync();
    }

}
