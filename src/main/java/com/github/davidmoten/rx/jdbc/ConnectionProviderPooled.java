package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Provides database connection pooling using HikariCP.
 */
public final class ConnectionProviderPooled implements ConnectionProvider {

    /**
     * HikariCP connection pool.
     */
    private final HikariDataSource pool;

    /**
     * Ensure idempotency of this.close() method.
     */
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    /**
     * Constructor.
     * 
     * @param url
     * @param minPoolSize
     * @param maxPoolSize
     */
    public ConnectionProviderPooled(String url, int minPoolSize, int maxPoolSize) {
        this(createPool(url, null, null, minPoolSize, maxPoolSize));
    }

    // Visible for testing
    ConnectionProviderPooled(HikariDataSource pool) {
        this.pool = pool;
    }

    /**
     * Constructor.
     * 
     * @param url
     *            jdbc url
     * @param username
     *            database login username
     * @param password
     *            database login password
     * @param minPoolSize
     *            minimum size of the connection pool
     * @param maxPoolSize
     *            maximum size of the connection pool
     */
    public ConnectionProviderPooled(String url, String username, String password, int minPoolSize,
            int maxPoolSize) {
        this(createPool(url, username, password, minPoolSize, maxPoolSize));
    }

    /**
     * Returns a new pooled data source based on jdbc url.
     * 
     * @param url
     *            jdbc url
     * @param username
     *            login username
     * @param password
     *            login password
     * @param minPoolSize
     *            minimum database connection pool size
     * @param maxPoolSize
     *            maximum database connection pool size
     * @return
     */
    private static HikariDataSource createPool(String url, String username, String password,
            int minPoolSize, int maxPoolSize) {

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setMinimumIdle(minPoolSize);
        ds.setMaximumPoolSize(maxPoolSize);
        return ds;
    }

    @Override
    public Connection get() {
        try {
            return pool.getConnection();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (isOpen.getAndSet(false))
            pool.close();
    }

}
