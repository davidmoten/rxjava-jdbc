package com.github.davidmoten.rx.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import java.sql.Connection;
import java.sql.SQLException;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class ConnectionProviderAutoCommittingTest {

    @Test
    public void testSqlExceptionThrowsRuntimeException() throws SQLException {
        ConnectionProvider cp = createMock(ConnectionProvider.class);
        Connection connection = createMock(Connection.class);
        expect(cp.get()).andReturn(connection).once();
        connection.setAutoCommit(true);
        expectLastCall().andThrow(new SQLException("boo"));
        replay(cp, connection);
        ConnectionProviderAutoCommitting c = new ConnectionProviderAutoCommitting(cp);
        try {
            c.get();
            Assert.fail();
        } catch (RuntimeException e) {
            EasyMock.verify(cp, connection);
        }
    }

}
