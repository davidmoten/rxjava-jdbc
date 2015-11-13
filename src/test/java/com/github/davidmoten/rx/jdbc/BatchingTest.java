package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import rx.Observable;

public class BatchingTest {

    @Test
    public void test() {
        Database db = DatabaseCreator.db();
        Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");
        int count = db.update("insert into person(name,score) values(?,0)")
                // set batch size
                .batchSize(3)
                // get parameters from last query
                .parameters(names).count().count().toBlocking().single();
        assertEquals(5, count);
    }

}
