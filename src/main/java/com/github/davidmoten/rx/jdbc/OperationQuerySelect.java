package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

class OperationQuerySelect {

	private static final Logger log = LoggerFactory
			.getLogger(OperationQuerySelect.class);

	/**
	 * Returns an Observable of the results of pushing one set of parameters
	 * through a select query.
	 * 
	 * @param params
	 *            one set of parameters to be run with the query
	 * @return
	 */
	static Observable<ResultSet> execute(QuerySelect query,
			List<Parameter> parameters) {
		return Observable.create(new QuerySelectOnSubscribe(query, parameters));
	}

	private static class QuerySelectOnSubscribe implements
			OnSubscribe<ResultSet> {

		private boolean keepGoing = true;
		private final List<Parameter> parameters;
		private final QuerySelect query;
		private Connection con;
		private PreparedStatement ps;
		private ResultSet rs;

		/**
		 * Constructor.
		 * 
		 * @param query
		 * @param parameters
		 */
		private QuerySelectOnSubscribe(QuerySelect query,
				List<Parameter> parameters) {
			this.query = query;
			this.parameters = parameters;
		}

		@Override
		public void call(Subscriber<? super ResultSet> subscriber) {
			try {
				connectAndPrepareStatement(subscriber);
				executeQuery(subscriber);
				while (keepGoing) {
					processRow(subscriber);
				}
				complete(subscriber);
			} catch (Exception e) {
				handleException(e, subscriber);
			} finally {
				close();
			}
		}

		/**
		 * Obtains connection, creates prepared statement and assigns parameters
		 * to the prepared statement.
		 * 
		 * @param subscriber
		 * 
		 * @throws SQLException
		 */
		private void connectAndPrepareStatement(
				Subscriber<? super ResultSet> subscriber) throws SQLException {
			log.debug("connectionProvider="
					+ query.context().connectionProvider());
			checkSubscription(subscriber);
			if (keepGoing) {
				log.debug("getting connection");
				con = query.context().connectionProvider().get();
				log.debug("preparing statement,sql=" + query.sql());
				ps = con.prepareStatement(query.sql());
				log.debug("setting parameters");
				Util.setParameters(ps, parameters);
			}
		}

		/**
		 * Executes the prepared statement.
		 * 
		 * @param subscriber
		 * 
		 * @throws SQLException
		 */
		private void executeQuery(Subscriber<? super ResultSet> subscriber)
				throws SQLException {
			checkSubscription(subscriber);
			if (keepGoing) {
				log.debug("executing ps");
				rs = ps.executeQuery();
				log.debug("executed ps=" + ps);
			}
		}

		/**
		 * Processes each row of the {@link ResultSet}.
		 * 
		 * @param subscriber
		 * 
		 * @throws SQLException
		 */
		private void processRow(Subscriber<? super ResultSet> subscriber)
				throws SQLException {
			checkSubscription(subscriber);
			if (!keepGoing)
				return;
			if (rs.next()) {
				log.trace("onNext");
				subscriber.onNext(rs);
			} else
				keepGoing = false;
		}

		/**
		 * Tells observer that stream is complete and closes resources.
		 * 
		 * @param subscriber
		 */
		private void complete(Subscriber<? super ResultSet> subscriber) {
			if (subscriber.isUnsubscribed()) {
				log.debug("unsubscribed");
			} else {
				log.debug("onCompleted");
				subscriber.onCompleted();
			}
		}

		/**
		 * Tells observer about exception.
		 * 
		 * @param e
		 * @param subscriber
		 */
		private void handleException(Exception e,
				Subscriber<? super ResultSet> subscriber) {
			log.debug("onError: " + e.getMessage());
			if (subscriber.isUnsubscribed())
				log.debug("unsubscribed");
			else {
				subscriber.onError(e);
			}
		}

		/**
		 * Closes connection resources (connection, prepared statement and
		 * result set).
		 */
		private void close() {
			log.debug("closing rs");
			Util.closeQuietly(rs);
			log.debug("closing ps");
			Util.closeQuietly(ps);
			log.debug("closing con");
			Util.closeQuietlyIfAutoCommit(con);
			log.debug("closed");
		}

		private void checkSubscription(Subscriber<? super ResultSet> subscriber) {
			if (subscriber.isUnsubscribed()) {
				keepGoing = false;
				log.debug("unsubscribing");
			}
		}

	}
}
