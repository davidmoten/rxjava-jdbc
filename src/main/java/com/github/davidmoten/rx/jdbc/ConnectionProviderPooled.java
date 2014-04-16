package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Provides database connection pooling using c3p0.
 */
public final class ConnectionProviderPooled implements ConnectionProvider {

    /**
     * C3p0 connection pool.
     */
    private final ComboPooledDataSource pool;

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
    ConnectionProviderPooled(ComboPooledDataSource pool) {
        this.pool = pool;
    }

    /**
     * Constructor.
     * 
     * @param url
     * @param username
     * @param password
     * @param minPoolSize
     * @param maxPoolSize
     */
    public ConnectionProviderPooled(String url, String username, String password, int minPoolSize,
            int maxPoolSize) {
        this(createPool(url, username, password, minPoolSize, maxPoolSize));
    }

    /**
     * Returns a new pooled data source based on jdbc url.
     * 
     * @param url
     * @param username
     * @param password
     * @param minPoolSize
     * @param maxPoolSize
     * @return
     */
    private static ComboPooledDataSource createPool(String url, String username, String password,
            int minPoolSize, int maxPoolSize) {
        ComboPooledDataSource pool = new ComboPooledDataSource();
        pool.setJdbcUrl(url);
        pool.setUser(username);
        pool.setPassword(password);
        pool.setMinPoolSize(minPoolSize);
        pool.setMaxPoolSize(maxPoolSize);
        pool.setAcquireIncrement(1);
        pool.setInitialPoolSize(0);
        return pool;
    }

    @Override
    public Connection get() {
        try {
            return pool.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (isOpen.getAndSet(false))
            pool.close();
    }

}
