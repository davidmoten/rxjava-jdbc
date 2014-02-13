package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

final class ConnectionProviderSingletonManualCommit implements
		ConnectionProvider {

	private Connection con;
	private final ConnectionProvider cp;

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

}
