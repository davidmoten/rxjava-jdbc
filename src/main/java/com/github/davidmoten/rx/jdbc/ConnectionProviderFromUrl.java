package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

/**
 * Provides {@link Connection}s from a url (using
 * DriverManager.getConnection()).
 */
public final class ConnectionProviderFromUrl implements ConnectionProvider {

    /**
     * JDBC url
     */
    private final String url;

    private final String username;

    private final String password;

    /**
     * Constructor.
     * 
     * @param url
     *            the jdbc url
     */
    public ConnectionProviderFromUrl(String url) {
        this(url, null, null);
    }

    /**
     * Constructor.
     * 
     * @param url
     *            jdbc url
     * @param username
     *            login username
     * @param password
     *            login password
     */
    public ConnectionProviderFromUrl(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection get() {
        try {
            if (username != null || password != null)
                return DriverManager.getConnection(url, username, password);
            else
                return DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
