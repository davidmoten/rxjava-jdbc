package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.concurrent.Executors;

import com.github.davidmoten.rx.jdbc.connection.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.SingletonManualCommitConnectionProvider;

/**
 * {@link QueryContext} for a transaction. Returns an executor based on a single
 * threaded pool and a cached non-auto commit {@link Connection}.
 */
class TransactionalQueryContext extends QueryContext {

	/**
	 * Constructor.
	 * 
	 * @param connectionProvider
	 */
	TransactionalQueryContext(ConnectionProvider connectionProvider) {
		super(Executors.newSingleThreadExecutor(),
				new SingletonManualCommitConnectionProvider(connectionProvider));
	}

}
