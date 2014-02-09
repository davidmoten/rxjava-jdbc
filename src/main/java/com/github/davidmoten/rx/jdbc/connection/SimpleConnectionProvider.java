package com.github.davidmoten.rx.jdbc.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SimpleConnectionProvider implements ConnectionProvider {

	private final String url;

	public SimpleConnectionProvider(String url) {
		this.url = url;
	}

	@Override
	public Connection get() {
		try {
			return DriverManager.getConnection(url);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
