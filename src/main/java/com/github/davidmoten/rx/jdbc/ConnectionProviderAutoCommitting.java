package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

/**
 * Provides {@link Connection}s with autoCommit set to true.
 */
final class ConnectionProviderAutoCommitting implements ConnectionProvider {

    /**
     * Underlying connection provider.
     */
    private final ConnectionProvider cp;

    /**
     * Constructor.
     * 
     * @param cp
     *            underlying connection provider
     */
    ConnectionProviderAutoCommitting(ConnectionProvider cp) {
        this.cp = cp;
    }

    @Override
    public Connection get() {
        Connection con = cp.get();
        try {
            con.setAutoCommit(true);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return con;
    }

    @Override
    public void close() {
        cp.close();
    }

}
