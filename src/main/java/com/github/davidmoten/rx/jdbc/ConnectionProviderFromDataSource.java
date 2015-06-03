package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class ConnectionProviderFromDataSource implements ConnectionProvider {

    private final DataSource dataSource;

    public ConnectionProviderFromDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection get() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

}
