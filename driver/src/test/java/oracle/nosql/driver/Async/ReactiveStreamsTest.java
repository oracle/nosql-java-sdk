/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.Async;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import oracle.nosql.driver.ProxyTestBase;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for consuming async APIs from reactive streams
 */
public class ReactiveStreamsTest extends ProxyTestBase {

    /**
     * Consuming Async Publisher from project reactor
     */
    @Test
    public void projectReactorTest() {
        final String userDDL = "create table if not exists users(" +
                "userId long, favorites array(string), primary key(userId))";
        final String userDrop = "drop table if exists users";

        final String suggestDDL = "create table if not exists suggestions(" +
                "sugId long, favorites array(string), primary key(sugId))";
        final String suggestDrop = "drop table if exists suggestions";
        final TableLimits limits = new TableLimits(500, 500, 50);


        /* Drop user table if already exists */
        StepVerifier.create(
            tableOperationAsync(asyncHandle, userDrop, null))
                .assertNext(tres -> {
                    assertNotNull(tres.getTableName());
                    assertNull(tres.getTableLimits());
                })
                .verifyComplete();

        /* Drop suggest table if already exists */
        StepVerifier.create(
            tableOperationAsync(asyncHandle, suggestDrop, null))
            .assertNext(tres -> {
                assertNotNull(tres.getTableName());
                assertNull(tres.getTableLimits());
            })
            .verifyComplete();

        /* create user table */
        StepVerifier.create(tableOperationAsync(asyncHandle, userDDL, limits))
            .assertNext(tres -> {
                assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                assertEquals("users", tres.getTableName());
                if (onprem) {
                    assertNull(tres.getTableLimits());
                }
                else {
                    assertEquals(limits.toString(),
                        tres.getTableLimits().toString());
                }
            })
            .verifyComplete();

        /* create suggestion table */
        StepVerifier.create(tableOperationAsync(asyncHandle, suggestDDL, limits))
            .assertNext(tres -> {
                assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                assertEquals("suggestions", tres.getTableName());
                if (onprem) {
                    assertNull(tres.getTableLimits());
                }
                else {
                    assertEquals(limits.toString(),
                        tres.getTableLimits().toString());
                }
            })
            .verifyComplete();

        /* insert rows to user table */
        PutRequest putRequest = new PutRequest().setValue(new MapValue()
                        .put("userId", 1)
                        .putFromJson("favorites",
                                "[\"1\", \"2\"]",
                                null))
                .setTableName("users");

        StepVerifier.create(asyncHandle.put(putRequest), 1)
                .assertNext(res ->  {
                    assertNotNull(res.getVersion());
                    assertWriteKB(res);
                })
                .thenRequest(1)
                .expectComplete()
                .verify();

        /* insert rows to suggestions table */
        putRequest = new PutRequest().setValue(new MapValue()
            .put("sugId", 1)
            .putFromJson("favorites",
                "[\"red\", \"black\", \"green\", \"orange\", \"yellow\", \"pink\"]",
                null))
            .setTableName("suggestions");

        StepVerifier.create(asyncHandle.put(putRequest), 1)
                .assertNext(res ->  {
                    assertNotNull(res.getVersion());
                    assertWriteKB(res);
                })
                .thenRequest(1)
                .expectComplete()
                .verify();

        Publisher<String> favPublisher = getUserFavs() //get user favs
            .timeout(Duration.ofMillis(1))
            .onErrorResume(t -> getSuggestFavs()) //onError switch to suggest
            .take(3); // take only first 3 elements

        StepVerifier.create(favPublisher)
            .expectNext("red")
            .expectNext("black")
            .expectNext("green")
            .expectComplete()
            .verify();
    }

    /**
     * Consuming Async Publisher from RxJava3
     */
    @Test
    public void rxJavaTest() {
        final String userDDL = "create table if not exists users(" +
                "userId long, favorites array(string), primary key(userId))";
        final String userDrop = "drop table if exists users";

        final String suggestDDL = "create table if not exists suggestions(" +
                "sugId long, favorites array(string), primary key(sugId))";
        final String suggestDrop = "drop table if exists suggestions";
        final TableLimits limits = new TableLimits(500, 500, 50);


        /* Drop user table if already exist */
        Single.fromPublisher(tableOperationAsync(asyncHandle, userDrop,
                null)).test().awaitCount(1)
        .assertValue(tres -> tres.getTableName() != null &&
            tres.getTableLimits() == null);


        /* Drop suggest table if already exists */
        Single.fromPublisher(tableOperationAsync(asyncHandle,
                        suggestDrop, null))
            .test()
            .awaitCount(1)
            .assertValue(tres ->
                tres.getTableName() != null &&
                tres.getTableLimits() == null
            )
            .assertComplete();


        /* create user table */
        Single.fromPublisher(tableOperationAsync(asyncHandle, userDDL, limits))
            .test()
            .awaitCount(1)
            .assertComplete();

        /* create suggestion table */
       Single.fromPublisher(tableOperationAsync(asyncHandle, suggestDDL, limits))
           .test()
           .awaitCount(1)
           .assertComplete();

        /* insert rows to user table */
        PutRequest putRequest = new PutRequest()
            .setValue(new MapValue()
                .put("userId", 1)
                .putFromJson("favorites",
                    "[\"1\", \"2\"]",
                    null))
            .setTableName("users");

        Single.fromPublisher(asyncHandle.put(putRequest))
            .test()
            .awaitCount(1)
            .assertComplete();

        /* insert rows to suggestions table */
        putRequest = new PutRequest()
            .setValue(new MapValue()
                .put("sugId", 1)
                .putFromJson("favorites",
                    "[\"red\", \"black\", \"green\", \"orange\", \"yellow\", \"pink\"]",
                    null))
            .setTableName("suggestions");

        Single.fromPublisher(asyncHandle.put(putRequest))
            .test()
            .awaitCount(1)
            .assertComplete();


        Observable.fromPublisher(getUserFavs())
            .timeout(1, TimeUnit.MILLISECONDS)
            .onErrorResumeNext(t -> Observable.fromPublisher(getSuggestFavs()))
            .take(3)
            .test()
            .awaitCount(3)
            .assertValueCount(3)
            .assertComplete();
    }

    @Test
    public void testTimeout() {
        ListTablesRequest lstRequest = new ListTablesRequest();
        StepVerifier.create(Flux.from(asyncHandle.listTables(lstRequest))
            .timeout(Duration.ofMillis(10)))
            .expectTimeout(Duration.ofSeconds(1));
    }

    private Flux<String> getUserFavs() {
        GetRequest favRequest = new GetRequest().setTableName("users")
            .setKey(new MapValue().put("userId", 1));

        return Mono.from(asyncHandle.get(favRequest))
            .flatMapMany(res -> {
                List<String> favList = new ArrayList<>();
                res.getValue().get("favorites").asArray().iterator()
                        .forEachRemaining(f -> favList.add(f.getString()));
                return Flux.fromStream(favList.stream());
            })
            .delayElements(Duration.ofMillis(500));
    }

    private Flux<String> getSuggestFavs() {
        GetRequest favRequest = new GetRequest().setTableName("suggestions")
                .setKey(new MapValue().put("sugId", 1));

        return Mono.from(asyncHandle.get(favRequest))
            .flatMapMany(res -> {
                List<String> favList = new ArrayList<>();
                res.getValue().get("favorites").asArray().iterator()
                        .forEachRemaining(f -> favList.add(f.getString()));
                return Flux.fromStream(favList.stream());
            });
    }

}
