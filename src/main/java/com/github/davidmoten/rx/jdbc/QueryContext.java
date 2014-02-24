package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
	Scheduler s = Schedulers.computation();

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
				Schedulers.executor(createSingleThreadExecutor()),
				new ConnectionProviderSingletonManualCommit(connectionProvider),
				handlers);
	}

	/**
	 * Creates a {@link ScheduledExecutorService} based on a single thread pool.
	 * 
	 * @return executor service based on single thread pool.
	 */
	private static ScheduledExecutorService createSingleThreadExecutor() {
		return Executors.newScheduledThreadPool(1, new ThreadFactory() {
			final AtomicInteger counter = new AtomicInteger();

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "TransactionThreadPool-"
						+ counter.incrementAndGet());
				t.setDaemon(true);
				return t;
			}
		});
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
			Handlers handlers) {

		return new QueryContext(Schedulers.computation(),
				new ConnectionProviderAutoCommitting(cp), handlers);
	}

	public Handlers handlers() {
		return handlers;
	}

}