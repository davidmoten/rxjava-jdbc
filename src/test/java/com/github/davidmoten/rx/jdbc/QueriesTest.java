package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action0;

import com.github.davidmoten.rx.jdbc.DatabaseTestBase.CountDownConnectionProvider;

public class QueriesTest {

    @Test
    public void obtainCoverageOfPrivateConstructor() {
        TestingUtil.instantiateUsingPrivateConstructor(Queries.class);
    }

    @Test
    public void testTwoConnectionsOpenedAndClosedWhenTakeOneUsedWithSelectThatReturnsOneRow()
            throws InterruptedException {
        Action0 completed = new Action0() {

            @Override
            public void call() {
                System.out.println("completed");
            }
        };
        CountDownConnectionProvider cp = new CountDownConnectionProvider(1, 1);
        Database db = new Database(cp);
        db.select("select count(*) from person").getAs(Long.class).doOnCompleted(completed).take(1)
                .toBlocking().single();
        assertTrue(cp.getsLatch().await(6, TimeUnit.SECONDS));
        assertTrue(cp.closesLatch().await(6, TimeUnit.SECONDS));
    }
    
    @Test
    public void testTakeOne() {
        Action0 completed = new Action0() {
            @Override
            public void call() {
                System.out.println("completed");
            }
        };
        Observable.from(Arrays.asList(1)).doOnUnsubscribe(completed).take(1).toBlocking().single();
    }

}
