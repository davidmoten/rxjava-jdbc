package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The threading and database connection context for mutliple jdbc queries.
 * 
 */
final class QueryContext {

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

	/**
	 * Returns a {@link QueryContext} suitable for running with a sequence of
	 * queries bounded by a database transaction.
	 * 
	 * @param connectionProvider
	 * @return
	 */
	static QueryContext newTransactionalQueryContext(
			ConnectionProvider connectionProvider) {
		return new QueryContext(Executors.newSingleThreadExecutor(),
				new ConnectionProviderSingletonManualCommit(connectionProvider));
	}

	/**
	 * Returns an asynchronous (outside database transactions)
	 * {@link QueryContext}.
	 * 
	 * @param cp
	 * @param threadPoolSize
	 * @return
	 */
	static QueryContext newAsynchronousQueryContext(ConnectionProvider cp,
			int threadPoolSize) {

		return new QueryContext(Executors.newFixedThreadPool(threadPoolSize),
				new ConnectionProviderAutoCommitting(cp));
	}

}