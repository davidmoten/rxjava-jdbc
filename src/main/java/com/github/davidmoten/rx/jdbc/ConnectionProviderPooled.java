package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import com.zaxxer.hikari.HikariDataSource;

import rx.functions.Action1;

/**
 * Provides database connection pooling using HikariCP.
 */
public final class ConnectionProviderPooled implements ConnectionProvider {

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 30000;

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
     * @param password
     * @param username
     * @param minPoolSize
     * @param maxPoolSize
     * @param connectionTimeoutMs
     */
    public ConnectionProviderPooled(String url, String username, String password, int minPoolSize,
            int maxPoolSize, long connectionTimeoutMs) {
        this(createPool(url, username, password, minPoolSize, maxPoolSize, connectionTimeoutMs));
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
        this(createPool(url, username, password, minPoolSize, maxPoolSize,
                DEFAULT_CONNECTION_TIMEOUT_MS));
    }

    public ConnectionProviderPooled(String url, int minPoolSize, int maxPoolSize) {
        this(createPool(url, null, null, minPoolSize, maxPoolSize, DEFAULT_CONNECTION_TIMEOUT_MS));
    }

    public ConnectionProviderPooled(String url, String username, String password, int minSize,
            int maxSize, long connectionTimeoutMs, Action1<HikariDataSource> configureDataSource) {
        this(configure(createPool(url, username, password, minSize, maxSize, connectionTimeoutMs),
                configureDataSource));
    }

    private static HikariDataSource configure(HikariDataSource ds,
            Action1<HikariDataSource> configure) {
        if (configure != null) {
            configure.call(ds);
        }
        return ds;
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
     * @param connectionTimeoutMs
     * @return
     */
    private static HikariDataSource createPool(String url, String username, String password,
            int minPoolSize, int maxPoolSize, long connectionTimeoutMs) {

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setMinimumIdle(minPoolSize);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setConnectionTimeout(connectionTimeoutMs);
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
