package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;

import java.util.Arrays;

public class BatchingTest {

    @Test
    public void test() {
        final Database db = DatabaseCreator.db();

        int numPeopleBefore = db.select("select count(*) from person").getAs(Integer.class)
                .toBlocking().single();
        Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");

        Observable<Integer> count = db.update("insert into person(name,score) values(?,0)")
                .dependsOn(db.beginTransaction())
                // set batch size
                .batchSize(3)
                .doOnBatchCommit(new Action1<int[]>() {
                    @Override
                    public void call(int[] integers) {
                        System.out.println(db.select("select count(*) from person").getAs(Integer.class)
                                .toBlocking().single());
                    }
                })
                // get parameters from last query
                .parameters(names)
                // go
                .count()
                // end transaction
                .count();
        assertTrue(db.commit(count).toBlocking().single());
        int numPeople = db.select("select count(*) from person").getAs(Integer.class).toBlocking()
                .single();
        assertEquals(numPeopleBefore + 5, numPeople);
    }

}
