package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.DatabaseCreator.connectionProvider;
import static com.github.davidmoten.rx.jdbc.DatabaseCreator.createDatabase;
import static java.util.Collections.nCopies;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import rx.Observable;

public class QueriesTest {

    private static final List<Integer> LIST_WITH_100_THREES = new ArrayList<>(nCopies(100, 3));

    @Test
    public void obtainCoverageOfPrivateConstructor() {
        TestingUtil.instantiateUsingPrivateConstructor(Queries.class);
    }

    @Test
    public void executeQueryUpdateWithDefaultBatchSize() {
        Connection con = connectionProvider().get();
        createDatabase(con);

        List<Integer> count = new QueryUpdate.Builder("update person set score = ?", Database.from(con))
                .parameters(Observable.range(1, 100))
                .count()
                .toList()
                .toBlocking()
                .single();

        assertThat(count, is(equalTo(LIST_WITH_100_THREES)));
    }

    @Test
    public void executeQueryUpdateWithBatchSizeOf10() {
        Connection con = connectionProvider().get();
        createDatabase(con);
        Database db = Database.from(con);

        List<Integer> count = new QueryUpdate.Builder("update person set score = ?", db)
                .parameters(Observable.range(1, 100))
                .batchSize(10)
                .count()
                .toList()
                .toBlocking()
                .single();

        assertThat(count, is(equalTo(LIST_WITH_100_THREES)));
    }

    @Test
    public void setBatchSizeGreaterThanQueryExecutionCount() {
        Connection con = connectionProvider().get();
        createDatabase(con);
        Database db = Database.from(con);

        List<Integer> count = new QueryUpdate.Builder("update person set score = ?", db)
                .parameters(Observable.range(1, 100))
                .batchSize(10000)
                .count()
                .toList()
                .toBlocking()
                .single();

        assertThat(count, is(equalTo(LIST_WITH_100_THREES)));
    }
}
