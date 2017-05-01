package com.github.davidmoten.rx.jdbc;

import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.mockito.Mockito;
import rx.schedulers.Schedulers;

public class FetchSizeTest {

  @Test
  public void testMocked() throws SQLException {
    String sql = "select name, score from people";
    Connection con = Mockito.mock(Connection.class);
    PreparedStatement ps = Mockito.mock(PreparedStatement.class);
    Mockito.when(con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)).thenReturn(ps);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(ps.executeQuery()).thenReturn(resultSet);

    Mockito.when(con.getAutoCommit()).thenReturn(false);
    Mockito.when(con.isClosed()).thenReturn(false);
    ConnectionProvider cp = createConnectionProvider(con);
    Database db = Database.from(cp);

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
