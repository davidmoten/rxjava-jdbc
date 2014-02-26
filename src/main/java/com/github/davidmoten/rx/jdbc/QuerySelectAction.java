package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;
import rx.Scheduler.Inner;
import rx.functions.Action1;

/**
 * Executes a select query (sql that returns a ResultSet). Can be cancelled by
 * calling the cancel() method.
 */
final public class QuerySelectAction implements Action1<Inner>, Cancellable {

	private static final Logger log = LoggerFactory
			.getLogger(QuerySelectAction.class);

	/**
	 * The select query to execute.
	 */
	private final QuerySelect query;

	private final List<Parameter> parameters;

	/**
	 * Object to synchronize on when manipulating connection resources.
	 */
	private final Object connectionLock = new Object();

	/**
	 * The connection.
	 */
	private volatile Connection con;

	/**
	 * The query statement.
	 */
	private volatile PreparedStatement ps;

	/**
	 * The results of running the query.
	 */
	private volatile ResultSet rs;
	private final Observer<? super ResultSet> o;
	private final AtomicBoolean keepGoing = new AtomicBoolean(true);

	/**
	 * Constructor.
	 * 
	 * @param query
	 * @param parameters
	 * @param o
	 */
	QuerySelectAction(QuerySelect query, List<Parameter> parameters,
			Observer<? super ResultSet> o) {
		this.query = query;
		this.parameters = parameters;
		this.o = o;
	}

	@Override
	public void call(Inner inner) {
		try {

			connectAndPrepareStatement();

			executeQuery();

			while (keepGoing.get()) {
				processRow();
			}

			complete();

		} catch (Exception e) {
			handleException(e);
		}
	}

	/**
	 * Obtains connection, creates prepared statement and assigns parameters to
	 * the prepared statement.
	 * 
	 * @throws SQLException
	 */
	private void connectAndPrepareStatement() throws SQLException {
		log.debug("connectionProvider=" + query.context().connectionProvider());
		synchronized (connectionLock) {
			if (keepGoing.get()) {
				log.debug("getting connection");
				con = query.context().connectionProvider().get();
				log.debug("preparing statement,sql=" + query.sql());
				ps = con.prepareStatement(query.sql());
				log.debug("setting parameters");
				Util.setParameters(ps, parameters);
			}
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
	 * @throws SQLException
	 */
	private void processRow() throws SQLException {
		synchronized (connectionLock) {
			if (rs.next()) {
				log.trace("onNext");
				o.onNext(rs);
			} else
				keepGoing.set(false);
		}
	}

	/**
	 * Tells observer that stream is complete and closes resources.
	 */
	private void complete() {
		log.debug("onCompleted");
		o.onCompleted();
		synchronized (connectionLock) {
			close();
		}
	}

	/**
	 * Tells observer about exception.
	 * 
	 * @param e
	 */
	private void handleException(Exception e) {
		log.debug("onError: " + e.getMessage());
		o.onError(e);
	}

	/**
	 * Closes connection resources (connection, prepared statement and result
	 * set).
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

	@Override
	public void cancel() {
		// will be called from another Thread to the run method so concurrency
		// controls are essential.
		synchronized (connectionLock) {
			keepGoing.set(false);
			close();
		}
	}

}
