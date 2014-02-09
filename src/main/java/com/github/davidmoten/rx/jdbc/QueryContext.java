package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.ExecutorService;

import com.github.davidmoten.rx.jdbc.connection.ConnectionProvider;

/**
 * The threading and database connection context for mutliple jdbc queries.
 * 
 */
class QueryContext {

	private final ExecutorService executor;
	private final ConnectionProvider connectionProvider;

	/**
	 * Constructor.
	 * 
	 * @param executor
	 * @param connectionProvider
	 */
	QueryContext(ExecutorService executor, ConnectionProvider connectionProvider) {
		this.executor = executor;
		this.connectionProvider = connectionProvider;
	}

	/**
	 * Returns the executor service to use to run queries with this context.
	 * 
	 * @return
	 */
	ExecutorService executor() {
		return executor;
	}

	/**
	 * Returns the connection provider for queries with this context.
	 * 
	 * @return
	 */
	ConnectionProvider connectionProvider() {
		return connectionProvider;
	}

}