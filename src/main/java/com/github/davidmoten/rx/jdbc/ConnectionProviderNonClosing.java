package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;

final class ConnectionProviderNonClosing implements ConnectionProvider {

    private final Connection con;

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
