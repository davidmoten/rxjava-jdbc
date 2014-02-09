package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.Executors;

import com.github.davidmoten.rx.jdbc.connection.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.SingletonManualCommitConnectionProvider;

public class TransactionalQueryContext extends QueryContext {

	TransactionalQueryContext(ConnectionProvider connectionProvider) {
		super(Executors.newSingleThreadExecutor(),
				new SingletonManualCommitConnectionProvider(connectionProvider));
	}

}
