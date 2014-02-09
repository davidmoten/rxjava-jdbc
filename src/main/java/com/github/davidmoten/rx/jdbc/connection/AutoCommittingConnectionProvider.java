package com.github.davidmoten.rx.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;

public class AutoCommittingConnectionProvider implements ConnectionProvider {

	private final ConnectionProvider cp;

	public AutoCommittingConnectionProvider(ConnectionProvider cp) {
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

}
