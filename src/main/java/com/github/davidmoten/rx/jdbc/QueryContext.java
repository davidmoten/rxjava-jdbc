package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;

import rx.Scheduler;

/**
 * The threading and database connection context for mutliple jdbc queries.
 * 
 */
final class QueryContext {

    /**
     * Provides {@link Connection}s.
     */
    private final ConnectionProvider connectionProvider;

    /**
     * Scheduler to use to execute query.
     */
    private final Scheduler scheduler;

    /**
     * Constructor.
     * 
     * @param executor
     * @param connectionProvider
     */
    QueryContext(Scheduler scheduler, ConnectionProvider connectionProvider) {
        this.scheduler = scheduler;
        this.connectionProvider = connectionProvider;
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
     * queries bounded by a database transaction. All queries running within a
     * transaction should use the same {@link Connection} and the same
     * {@link Thread} to execute on.
     * 
     * @param connectionProvider
     * @return
     */
    static QueryContext newTransactionalQueryContext(ConnectionProvider connectionProvider, Scheduler scheduler) {
        return new QueryContext(scheduler, new ConnectionProviderSingletonManualCommit(connectionProvider));
    }

    /**
     * Returns an asynchronous (outside database transactions)
     * {@link QueryContext}.
     * 
     * @param cp
     * @param threadPoolSize
     * @return
     */
    static QueryContext newNonTransactionalQueryContext(ConnectionProvider cp, Scheduler scheduler) {

        return new QueryContext(scheduler, new ConnectionProviderAutoCommitting(cp));
    }

}