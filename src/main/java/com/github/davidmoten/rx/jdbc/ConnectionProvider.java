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

    /**
     * Closes the connection provider and releases its resources. For example, a
     * connection pool may need formal closure to release its connections
     * because connection.close() is actually just releasing a connection back
     * to the pool for reuse. This method should be idempotent.
     */
    void close();

}
