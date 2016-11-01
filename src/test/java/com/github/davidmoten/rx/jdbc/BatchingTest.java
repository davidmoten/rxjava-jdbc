package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.github.davidmoten.rx.Actions;
import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import com.github.davidmoten.rx.testing.TestingHelper;

import rx.Observable;
import rx.functions.Func1;

public final class BatchingTest {

    @Test
    public void testUnmocked() {
        Database db = DatabaseCreator.db();
        int numPeopleBefore = db.select("select count(*) from person") //
                .getAs(Integer.class) //
                .toBlocking().single();
        Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");

        Observable<Integer> count = db.update("insert into person(name,score) values(?,0)")
                .dependsOn(db.beginTransaction())
                // set batch size
                .batchSize(3)
                // get parameters from last query
                .parameters(names)
                // go
                .count()
                // end transaction
                .count();
        assertTrue(db.commit(count).toBlocking().single());
        int numPeople = db.select("select count(*) from person") //
                .getAs(Integer.class) //
                .toBlocking().single();
        assertEquals(numPeopleBefore + 5, numPeople);
    }

    @Test
    public void testBatchingCanOnlyBeUsedWithinATransaction() {
        Database db = DatabaseCreator.db();
        Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");

        Observable<Integer> count = db.update("insert into person(name,score) values(?,0)")
                // set batch size
                .batchSize(3)
                // get parameters from last query
                .parameters(names)
                // go
                .count().count();
        count //
                .to(TestingHelper.<Integer> test()) //
                .assertError(SQLRuntimeException.class);
    }

    @Test
    public void testMocked() throws SQLException {
        String sql = "insert into person(name,score) values(?, 0)";
        final Connection con = Mockito.mock(Connection.class);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(con.prepareStatement(sql, Statement.NO_GENERATED_KEYS)).thenReturn(ps);
        Mockito.when(ps.executeBatch()) //
                .thenReturn(new int[] { 1, 2, 3 }) //
                .thenReturn(new int[] { 4, 5 });
        Mockito.when(con.getAutoCommit()).thenReturn(false);
        Mockito.when(con.isClosed()).thenReturn(false);
        ConnectionProvider cp = createConnectionProvider(con);
        Database db = Database.from(cp);
        Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");
        AtomicInteger records = new AtomicInteger();
        Observable<Integer> count = db.update(sql) //
                .dependsOn(db.beginTransaction())
                // set batch size
                .batchSize(3)
                // get parameters from last query
                .parameters(names)
                // go
                .count()
                // end transaction
                .toList()
                // sum record counts
                .map(new Func1<List<Integer>, Integer>() {
                    @Override
                    public Integer call(List<Integer> list) {
                        return sum(list);
                    }
                })
                // set result to variable
                .doOnNext(Actions.setAtomic(records)) //
                .count();
        db.commit(count).toBlocking().single();
        InOrder in = Mockito.inOrder(con, ps);
        in.verify(con, Mockito.times(1)).prepareStatement(sql, Statement.NO_GENERATED_KEYS);
        in.verify(ps, Mockito.times(1)).setObject(1, "NANCY");
        in.verify(ps, Mockito.times(1)).addBatch();
        in.verify(ps, Mockito.times(1)).setObject(1, "WARREN");
        in.verify(ps, Mockito.times(1)).addBatch();
        in.verify(ps, Mockito.times(1)).setObject(1, "ALFRED");
        in.verify(ps, Mockito.times(1)).addBatch();
        in.verify(ps, Mockito.times(1)).executeBatch();
        in.verify(ps, Mockito.times(1)).setObject(1, "BARRY");
        in.verify(ps, Mockito.times(1)).addBatch();
        in.verify(ps, Mockito.times(1)).setObject(1, "ROBERTO");
        in.verify(ps, Mockito.times(1)).addBatch();
        in.verify(ps, Mockito.times(1)).executeBatch();
        // in.verify(con, Mockito.times(1)).commit();
        in.verify(con, Mockito.times(1)).isClosed();
        in.verify(con, Mockito.times(1)).close();
        in.verifyNoMoreInteractions();
        assertFalse(db.connectionProvider() instanceof ConnectionProviderBatch);
        assertEquals(1 + 2 + 3 + 4 + 5, records.get());
    }

    private static int sum(List<Integer> list) {
        int sum = 0;
        for (Integer n : list) {
            sum += n;
        }
        return sum;
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotReturnGeneratedKeysWhenBatching() {
        Database db = DatabaseCreator.db();
        Observable<String> names = Observable.just("NANCY");

        db.update("insert into person(name,score) values(?,0)").dependsOn(db.beginTransaction())
                // set batch size
                .batchSize(3)
                // get parameters from last query
                .parameters(names)
                //
                .returnGeneratedKeys();
    }

    private static ConnectionProvider createConnectionProvider(final Connection con) {
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                return con;
            }

            @Override
            public void close() {

            }
        };
    }

}
