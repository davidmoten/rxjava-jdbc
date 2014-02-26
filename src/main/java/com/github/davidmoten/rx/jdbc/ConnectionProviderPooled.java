package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Provides database connection pooling using c3p0.
 */
public class ConnectionProviderPooled implements ConnectionProvider {

	private final ComboPooledDataSource pool;

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
		pool.close();
	}

}
