package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;

/**
 * Provides JDBC Connections as required. It is advisable generally to use a
 * Connection pool.
 * 
 **/
public interface ConnectionProvider {

	/**
	 * Returns a new {@link Connection} (perhaps from a Connection pool).
	 * 
	 * @return a new Connection to a database
	 */
	Connection get();

	void close();

}
