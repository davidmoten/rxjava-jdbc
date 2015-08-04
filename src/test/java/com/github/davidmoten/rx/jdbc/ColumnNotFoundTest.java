package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import rx.observers.TestSubscriber;

import com.github.davidmoten.rx.jdbc.Database;
import com.github.davidmoten.rx.jdbc.annotations.Column;
import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public class ColumnNotFoundTest {

    static interface Thing {

        @Column("bingo")
        Long hello();

    }

    @Test
    public void test() {
        TestSubscriber<Object> ts = new TestSubscriber<Object>();
        Database db = DatabaseCreator.db();
        db.select("select name from Person").autoMap(Thing.class).count().subscribe(ts);
        ts.assertError(SQLRuntimeException.class);
        assertTrue(ts.getOnErrorEvents().get(0).getMessage()
                .startsWith("query column names do not include 'bingo'"));
    }

}
