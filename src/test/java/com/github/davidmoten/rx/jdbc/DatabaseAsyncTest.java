package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Observable;
import rx.functions.Func2;

public class DatabaseAsyncTest extends DatabaseTestBase {

    public DatabaseAsyncTest() {
        super(true);
    }

    @Test
    public void testDependsUsingAsynchronousQueriesWaitsForFirstByDelayingCalculation() {
        Database db = db().asynchronous();
        Observable<Integer> insert = db.update("insert into person(name,score) values(?,?)")
                .parameters("JOHN", 45).count()
                .zipWith(Observable.interval(100, TimeUnit.MILLISECONDS),
                        new Func2<Integer, Long, Integer>() {
                            @Override
                            public Integer call(Integer t1, Long t2) {
                                return t1;
                            }
                        });

        Observable<Integer> count = db.select("select name from person").dependsOn(insert).count();
        assertIs(4, count);
    }

}
