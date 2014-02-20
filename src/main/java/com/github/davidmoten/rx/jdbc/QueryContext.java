package com.github.davidmoten.rx.jdbc;

import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * The threading and database connection context for mutliple jdbc queries.
 * 
 */
final class QueryContext {

	private final ConnectionProvider connectionProvider;
	private final Handlers handlers;
	private final Scheduler scheduler;

	/**
	 * Constructor.
	 * 
	 * @param executor
	 * @param connectionProvider
	 */
	QueryContext(Scheduler scheduler, ConnectionProvider connectionProvider,
			Handlers handlers) {
		this.scheduler = scheduler;
		this.connectionProvider = connectionProvider;
		this.handlers = handlers;
	}

	/**
	 * Returns the scheduler service to use to run queries with this context.
	 * 
	 * @return
	 */
	Scheduler scheduler() {
		return scheduler;
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
			ConnectionProvider connectionProvider, Handlers handlers) {
		return new QueryContext(
				Schedulers.currentThread(),
				new ConnectionProviderSingletonManualCommit(connectionProvider),
				handlers);
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
			int threadPoolSize, Handlers handlers) {

		return new QueryContext(Schedulers.computation(),
				new ConnectionProviderAutoCommitting(cp), handlers);
	}

	public Handlers handlers() {
		return handlers;
	}

}