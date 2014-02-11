package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import rx.Observer;

/**
 * Executes an update query.
 */
public class QueryUpdateRunnable implements Runnable, Cancellable {

	/**
	 * Logger.
	 */
	private static final Logger log = Logger
			.getLogger(QueryUpdateRunnable.class);

	/**
	 * Special sql command that brings about a rollback.
	 */
	private static final String ROLLBACK = "rollback";

	/**
	 * Special sql command that brings about a commit.
	 */
	private static final String COMMIT = "commit";

	/**
	 * The query to be executed.
	 */
	private final QueryUpdate query;

	/**
	 * The query parameters.
	 */
	private final List<Parameter> parameters;

	/**
	 * Locks connection resources (connection, statement and resultset) to avoid
	 * strange behaviour under concurrent access.
	 */
	private final Object connectionLock = new Object();

	/**
	 * Query connection.
	 */
	private volatile Connection con;
	/**
	 * Prepared statement for the query.
	 */
	private volatile PreparedStatement ps;

	/**
	 * The observer to be notified of the results of the query.
	 */
	private final Observer<? super Integer> observer;

	/**
	 * Constructor.
	 * 
	 * @param query
	 * @param parameters
	 * @param observer
	 */
	public QueryUpdateRunnable(QueryUpdate query, List<Parameter> parameters,
			Observer<? super Integer> observer) {
		this.query = query;
		this.parameters = parameters;
		this.observer = observer;
	}

	@Override
	public void run() {
		try {
			con = query.context().connectionProvider().get();
			log.debug("cp=" + query.context().connectionProvider());
			if (query.sql().equals(COMMIT))
				performCommit();
			else if (query.sql().equals(ROLLBACK))
				performRollback();
			else
				performUpdate();
			log.debug("onCompleted");
			observer.onCompleted();
		} catch (Exception e) {
			log.debug("onError: " + e.getMessage());
			observer.onError(e);
		}
	}

	/**
	 * Rolls back the current transaction. Throws {@link RuntimeException} if
	 * connection is in autoCommit mode.
	 */
	private void performRollback() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.rollback(con);
			observer.onNext(Integer.valueOf(0));
		}
	}

	/**
	 * Commits the current transaction. Throws {@link RuntimeException} if
	 * connection is in autoCommit mode.
	 */
	private void performCommit() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.commit(con);
			observer.onNext(Integer.valueOf(1));
		}
	}

	/**
	 * Executes the prepared statement.
	 * 
	 * @throws SQLException
	 */
	private void performUpdate() throws SQLException {
		synchronized (connectionLock) {
			ps = con.prepareStatement(query.sql());
			Util.setParameters(ps, parameters);
		}
		log.debug("executing ps=" + ps);
		int count = ps.executeUpdate();
		log.debug("executed ps=" + ps);
		log.debug("onNext");
		observer.onNext((count));
		synchronized (connectionLock) {
			close();
		}
	}

	/**
	 * Cancels a running PreparedStatement, closing it and the current
	 * Connection but only if auto commit mode.
	 */
	private void close() {
		Util.closeQuietly(ps);
		Util.closeQuietlyIfAutoCommit(con);
	}

	@Override
	public void cancel() {
		// will be called from another Thread to the run method so concurrency
		// controls are essential.
		synchronized (connectionLock) {
			close();
		}
	}

}
