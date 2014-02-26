package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;
import rx.Scheduler.Inner;
import rx.functions.Action1;

/**
 * Executes an update query.
 */
final public class QueryUpdateAction implements Action1<Inner>, Cancellable {

	/**
	 * Logger.
	 */
	private static final Logger log = LoggerFactory
			.getLogger(QueryUpdateAction.class);

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
	 * The parameters to run the query against (may be a subset of the query
	 * parameters specified in the query because the query may be run multiple
	 * times with multiple sets of parameters).
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
	public QueryUpdateAction(QueryUpdate query, List<Parameter> parameters,
			Observer<? super Integer> observer) {
		this.query = query;
		this.parameters = parameters;
		this.observer = observer;
	}

	@Override
	public void call(Inner inner) {
		try {

			getConnection();

			if (isCommit())
				performCommit();
			else if (isRollback())
				performRollback();
			else
				performUpdate();

			complete();

		} catch (Exception e) {
			handleException(e);
		}
	}

	/**
	 * Gets the current connection.
	 */
	private void getConnection() {
		log.info("getting connection");
		con = query.context().connectionProvider().get();
		log.debug("cp=" + query.context().connectionProvider());
	}

	/**
	 * Returns true if and only if the sql statement is a commit command.
	 * 
	 * @return if is commit
	 */
	private boolean isCommit() {
		return query.sql().equals(COMMIT);
	}

	/**
	 * Returns true if and only if the sql statement is a rollback command.
	 * 
	 * @return if is rollback
	 */
	private boolean isRollback() {
		return query.sql().equals(ROLLBACK);
	}

	/**
	 * Commits the current transaction. Throws {@link RuntimeException} if
	 * connection is in autoCommit mode.
	 */
	private void performCommit() {
		log.debug("committing");
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.commit(con);
			observer.onNext(Integer.valueOf(1));
		}
		log.debug("committed");
	}

	/**
	 * Rolls back the current transaction. Throws {@link RuntimeException} if
	 * connection is in autoCommit mode.
	 */
	private void performRollback() {
		log.debug("rolling back");
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.rollback(con);
			observer.onNext(Integer.valueOf(0));
		}
		log.debug("rolled back");
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
	 * Notify observer that sequence is complete.
	 */
	private void complete() {
		log.debug("onCompleted");
		observer.onCompleted();
	}

	/**
	 * Notify observer of an error.
	 * 
	 * @param e
	 */
	private void handleException(Exception e) {
		log.debug("onError: " + e.getMessage());
		observer.onError(e);
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
