package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Provides database connection pooling using c3p0.
 */
public class ConnectionProviderPooled implements ConnectionProvider {

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
		pool = new ComboPooledDataSource();
		pool.setJdbcUrl(url);
		pool.setMinPoolSize(minPoolSize);
		pool.setMaxPoolSize(maxPoolSize);
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
