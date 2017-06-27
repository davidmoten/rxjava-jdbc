package com.github.davidmoten.rx.jdbc;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import rx.schedulers.Schedulers;

public class FetchSizeTest {

  private String sql;

  private Database db;

  private PreparedStatement ps;

  @Before
  public void setup() throws Exception {
    sql = "select name, score from people";
    Connection con = Mockito.mock(Connection.class);
    ps = Mockito.mock(PreparedStatement.class);
    Mockito.when(con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)).thenReturn(ps);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(ps.executeQuery()).thenReturn(resultSet);

    Mockito.when(con.getAutoCommit()).thenReturn(false);
    Mockito.when(con.isClosed()).thenReturn(false);
    ConnectionProvider cp = createConnectionProvider(con);
    db = Database.from(cp);
  }

  @Test
  public void testSetFetchSizePositive() throws SQLException {
    db.select(sql) //
        // set batch size
        .fetchSize(3)
        // go
        .count()
        //
        .subscribeOn(Schedulers.immediate())
        // go
        .subscribe();

    verify(ps, Mockito.times(1)).setFetchSize(3);
  }

  @Test
  public void testSetFetchSizeMinInteger() throws SQLException {
    db.select(sql) //
        // set batch size
        .fetchSize(Integer.MIN_VALUE)
        // go
        .count()
        //
        .subscribeOn(Schedulers.immediate())
        // go
        .subscribe();

    verify(ps, Mockito.times(1)).setFetchSize(Integer.MIN_VALUE);
  }

  @Test
  public void testSetFetchSizeMaxInteger() throws SQLException {
    db.select(sql) //
        // set batch size
        .fetchSize(Integer.MAX_VALUE)
        // go
        .count()
        //
        .subscribeOn(Schedulers.immediate())
        // go
        .subscribe();

    verify(ps, Mockito.times(1)).setFetchSize(Integer.MAX_VALUE);
  }

  @Test
  public void testNotSetFetchSize() throws SQLException {
    db.select(sql) //
        // go
        .count()
        //
        .subscribeOn(Schedulers.immediate())
        // go
        .subscribe();

    verify(ps, Mockito.never()).setFetchSize(any(Integer.class));
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
