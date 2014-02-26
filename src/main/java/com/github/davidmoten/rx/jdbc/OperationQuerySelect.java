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
	static Observable<ResultSet> executeOnce(QuerySelect query,
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

		QuerySelectOnSubscribe(QuerySelect query, List<Parameter> parameters) {
			this.query = query;
			this.parameters = parameters;
		}

		@Override
		public void call(Subscriber<? super ResultSet> o) {
			try {

				checkSubscription(o);
				connectAndPrepareStatement(o);

				checkSubscription(o);
				executeQuery();

				checkSubscription(o);
				while (keepGoing) {
					processRow(o);
					checkSubscription(o);
				}

				complete(o);

			} catch (Exception e) {
				handleException(e, o);
			}

		}

		private void checkSubscription(Subscriber<? super ResultSet> o) {
			if (o.isUnsubscribed())
				keepGoing = false;
		}

		/**
		 * Obtains connection, creates prepared statement and assigns parameters
		 * to the prepared statement.
		 * 
		 * @param o
		 * 
		 * @throws SQLException
		 */
		private void connectAndPrepareStatement(Subscriber<? super ResultSet> o)
				throws SQLException {
			log.debug("connectionProvider="
					+ query.context().connectionProvider());
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
		 * @throws SQLException
		 */
		private void executeQuery() throws SQLException {
			log.debug("executing ps");
			rs = ps.executeQuery();
			log.debug("executed ps=" + ps);
		}

		/**
		 * Processes each row of the {@link ResultSet}.
		 * 
		 * @param o
		 * 
		 * @throws SQLException
		 */
		private void processRow(Subscriber<? super ResultSet> o)
				throws SQLException {
			if (rs.next()) {
				log.trace("onNext");
				o.onNext(rs);
			} else
				keepGoing = false;
		}

		/**
		 * Tells observer that stream is complete and closes resources.
		 * 
		 * @param o
		 */
		private void complete(Subscriber<? super ResultSet> o) {
			log.debug("onCompleted");
			o.onCompleted();
			close();
		}

		/**
		 * Tells observer about exception.
		 * 
		 * @param e
		 * @param o
		 */
		private void handleException(Exception e,
				Subscriber<? super ResultSet> o) {
			log.debug("onError: " + e.getMessage());
			o.onError(e);
			close();
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

	}
}
