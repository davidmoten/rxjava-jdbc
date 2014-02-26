package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

class OperationQueryUpdate {

	private static final Logger log = LoggerFactory
			.getLogger(OperationQueryUpdate.class);

	/**
	 * Returns an Observable of the results of pushing one set of parameters
	 * through a select query.
	 * 
	 * @param params
	 *            one set of parameters to be run with the query
	 * @return
	 */
	static Observable<Integer> execute(QueryUpdate query,
			List<Parameter> parameters) {
		return Observable.create(new QueryUpdateOnSubscribe(query, parameters));
	}

	private static class QueryUpdateOnSubscribe implements OnSubscribe<Integer> {

		private boolean keepGoing = true;
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
		 * parameters specified in the query because the query may be run
		 * multiple times with multiple sets of parameters).
		 */
		private final List<Parameter> parameters;

		/**
		 * Query connection.
		 */
		private Connection con;

		/**
		 * Prepared statement for the query.
		 */
		private PreparedStatement ps;

		QueryUpdateOnSubscribe(QueryUpdate query, List<Parameter> parameters) {
			this.query = query;
			this.parameters = parameters;
		}

		@Override
		public void call(Subscriber<? super Integer> observer) {
			try {

				getConnection();

				if (isCommit())
					performCommit(observer);
				else if (isRollback())
					performRollback(observer);
				else
					performUpdate(observer);

				complete(observer);

			} catch (Exception e) {
				handleException(e, observer);
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
		 * 
		 * @param observer
		 */
		private void performCommit(Subscriber<? super Integer> observer) {
			checkSubscription(observer);
			if (!keepGoing)
				return;

			log.debug("committing");
			Conditions.checkTrue(!Util.isAutoCommit(con));
			Util.commit(con);

			checkSubscription(observer);
			if (!keepGoing)
				return;

			observer.onNext(Integer.valueOf(1));
			log.debug("committed");
		}

		/**
		 * Rolls back the current transaction. Throws {@link RuntimeException}
		 * if connection is in autoCommit mode.
		 * 
		 * @param observer
		 */
		private void performRollback(Subscriber<? super Integer> observer) {
			log.debug("rolling back");
			Conditions.checkTrue(!Util.isAutoCommit(con));
			Util.rollback(con);
			observer.onNext(Integer.valueOf(0));
			log.debug("rolled back");
		}

		/**
		 * Executes the prepared statement.
		 * 
		 * @param observer
		 * 
		 * @throws SQLException
		 */
		private void performUpdate(Subscriber<? super Integer> observer)
				throws SQLException {
			checkSubscription(observer);
			if (!keepGoing)
				return;

			ps = con.prepareStatement(query.sql());
			Util.setParameters(ps, parameters);

			checkSubscription(observer);
			if (!keepGoing)
				return;

			log.debug("executing ps=" + ps);
			int count = ps.executeUpdate();
			log.debug("executed ps=" + ps);
			log.debug("onNext");

			checkSubscription(observer);
			if (!keepGoing)
				return;

			observer.onNext((count));
			close();
		}

		/**
		 * Notify observer that sequence is complete.
		 * 
		 * @param observer
		 */
		private void complete(Subscriber<? super Integer> observer) {
			log.debug("onCompleted");
			observer.onCompleted();
		}

		/**
		 * Notify observer of an error.
		 * 
		 * @param e
		 * @param observer
		 */
		private void handleException(Exception e,
				Subscriber<? super Integer> observer) {
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

		private void checkSubscription(Subscriber<? super Integer> o) {
			if (o.isUnsubscribed()) {
				keepGoing = false;
				log.debug("unsubscribing");
			}
		}

	}
}
