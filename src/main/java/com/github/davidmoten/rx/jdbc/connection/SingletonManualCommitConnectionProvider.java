package com.github.davidmoten.rx.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;

public class SingletonManualCommitConnectionProvider implements
		ConnectionProvider {

	private Connection con;
	private ConnectionProvider cp;

	public SingletonManualCommitConnectionProvider(ConnectionProvider cp) {
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
