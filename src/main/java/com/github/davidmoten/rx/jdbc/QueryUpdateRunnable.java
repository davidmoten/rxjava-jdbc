package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import rx.Observer;

public class QueryUpdateRunnable<T> implements Runnable, Cancellable {

	private static final String ROLLBACK = "rollback";
	private static final String COMMIT = "commit";

	private static final Logger log = Logger
			.getLogger(QueryUpdateRunnable.class);

	private final Object connectionLock = new Object();
	private volatile Connection con;
	private volatile PreparedStatement ps;
	private volatile ResultSet rs;
	private final QueryUpdate<T> query;
	private final List<Object> params;
	private final Observer<? super T> o;

	public QueryUpdateRunnable(QueryUpdate<T> query, List<Object> params,
			Observer<? super T> o) {
		this.query = query;
		this.params = params;
		this.o = o;
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
			o.onCompleted();
		} catch (Exception e) {
			log.debug("onError: " + e.getMessage());
			o.onError(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void performRollback() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.rollback(con);
			o.onNext((T) Integer.valueOf(0));
		}
	}

	@SuppressWarnings("unchecked")
	private void performCommit() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.commit(con);
			o.onNext((T) Integer.valueOf(1));
		}
	}

	@SuppressWarnings("unchecked")
	private void performUpdate() throws SQLException {
		synchronized (connectionLock) {
			ps = con.prepareStatement(query.sql());
			Util.setParameters(ps, params);
		}
		log.debug("executing ps=" + ps);
		int count = ps.executeUpdate();
		log.debug("executed ps=" + ps);
		log.debug("onNext");
		o.onNext((T) ((Integer) count));
		synchronized (connectionLock) {
			close();
		}
	}

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
			close();
		}
	}

}
