package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides a singleton {@link Connection} sourced from a
 * {@link ConnectionProvider} that has autoCommit set to false.
 */
final class ConnectionProviderSingletonManualCommit implements
		ConnectionProvider {

	/**
	 * Singleton connection.
	 */
	private Connection con;

	/**
	 * Provides the singleton connection.
	 */
	private final ConnectionProvider cp;

	/**
	 * Constructor.
	 * 
	 * @param cp
	 *            connection provider.
	 */
	ConnectionProviderSingletonManualCommit(ConnectionProvider cp) {
		this.cp = cp;
	}

	@Override
	public synchronized Connection get() {
		if (con == null) {
			con = cp.get();
			try {
				con.setAutoCommit(false);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return con;
	}

	@Override
	public void close() {
		cp.close();
	}

}