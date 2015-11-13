package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

/**
 * Provides a singleton {@link Connection} sourced from a
 * {@link ConnectionProvider} that has autoCommit set to false.
 */
final class ConnectionProviderSingletonManualCommitStatementCaching implements ConnectionProvider {

    /**
     * Singleton connection.
     */
    private Connection con;

    /**
     * Ensures thread-safe setting of con
     */
    private final AtomicBoolean connectionSet = new AtomicBoolean(false);

    /**
     * Provides the singleton connection.
     */
    private final ConnectionProvider cp;

    /**
     * Constructor.
     * 
     * @param cp
     *            connection provider.
     */
    ConnectionProviderSingletonManualCommitStatementCaching(ConnectionProvider cp) {
        this.cp = cp;
    }

    @Override
    public Connection get() {
        if (connectionSet.compareAndSet(false, true)) {
            con = cp.get();
            try {
                con.setAutoCommit(false);
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
        }
        return new ConnectionStatementCaching(con);
    }

    @Override
    public void close() {
        cp.close();
    }

}