package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Provides {@link Connection}s from a url (using
 * DriverManager.getConnection()).
 */
public class ConnectionProviderFromUrl implements ConnectionProvider {

    /**
     * JDBC url
     */
    private final String url;

    /**
     * Constructor.
     * 
     * @param url
     *            the jdbc url
     */
    public ConnectionProviderFromUrl(String url) {
        this.url = url;
    }

    @Override
    public Connection get() {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
