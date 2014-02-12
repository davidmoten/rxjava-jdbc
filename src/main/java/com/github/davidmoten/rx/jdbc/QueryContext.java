package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.davidmoten.rx.jdbc.connection.AutoCommittingConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.SingletonManualCommitConnectionProvider;

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
				new SingletonManualCommitConnectionProvider(connectionProvider));
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
				new AutoCommittingConnectionProvider(cp));
	}

}