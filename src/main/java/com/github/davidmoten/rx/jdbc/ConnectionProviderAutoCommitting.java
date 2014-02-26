package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

final class ConnectionProviderAutoCommitting implements ConnectionProvider {

	private final ConnectionProvider cp;

	ConnectionProviderAutoCommitting(ConnectionProvider cp) {
		this.cp = cp;
	}

	@Override
	public Connection get() {
		Connection con = cp.get();
		try {
			con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return con;
	}

	@Override
	public void close() {
		cp.close();
	}

}
