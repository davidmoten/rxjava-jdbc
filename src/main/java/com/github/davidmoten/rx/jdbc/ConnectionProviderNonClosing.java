package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;

/**
 * Wraps a single {@link Connection} so that calls to {@link Connection#close()}
 * are ignored.
 */
final class ConnectionProviderNonClosing implements ConnectionProvider {

    private final Connection con;

    /**
     * Constructor.
     * 
     * @param con
     */
    public ConnectionProviderNonClosing(Connection con) {
        this.con = con;
    }

    @Override
    public Connection get() {
        return new ConnectionNonClosing(con);
    }

    @Override
    public void close() {
        // do nothing
    }

}
