package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observer;

/**
 * Executes a select query (sql that returns a ResultSet). Can be cancelled by
 * calling the cancel() method.
 */
class QuerySelectRunnable implements Runnable, Cancellable {

	private static final Logger log = Logger
			.getLogger(QuerySelectRunnable.class);

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
	QuerySelectRunnable(QuerySelect query, List<Parameter> parameters,
			Observer<? super ResultSet> o) {
		this.query = query;
		this.parameters = parameters;
		this.o = o;
	}

	@Override
	public void run() {
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
		log.debug(query.context().connectionProvider());
		synchronized (connectionLock) {
			if (keepGoing.get()) {
				con = query.context().connectionProvider().get();
				ps = con.prepareStatement(query.sql());
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
				log.debug("onNext");
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
		Util.closeQuietly(rs);
		Util.closeQuietly(ps);
		Util.closeQuietlyIfAutoCommit(con);
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
