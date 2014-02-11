package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import rx.Observer;

public class QueryUpdateRunnable implements Runnable, Cancellable {

	private static final String ROLLBACK = "rollback";
	private static final String COMMIT = "commit";

	private static final Logger log = Logger
			.getLogger(QueryUpdateRunnable.class);

	private final Object connectionLock = new Object();
	private volatile Connection con;
	private volatile PreparedStatement ps;
	private volatile ResultSet rs;
	private final QueryUpdate query;
	private final List<Parameter> params;
	private final Observer<? super Integer> o;

	public QueryUpdateRunnable(QueryUpdate query, List<Parameter> params,
			Observer<? super Integer> o) {
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

	private void performRollback() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.rollback(con);
			o.onNext(Integer.valueOf(0));
		}
	}

	private void performCommit() {
		Conditions.checkTrue(!Util.isAutoCommit(con));
		synchronized (connectionLock) {
			Util.commit(con);
			o.onNext(Integer.valueOf(1));
		}
	}

	private void performUpdate() throws SQLException {
		synchronized (connectionLock) {
			ps = con.prepareStatement(query.sql());
			Util.setParameters(ps, params);
		}
		log.debug("executing ps=" + ps);
		int count = ps.executeUpdate();
		log.debug("executed ps=" + ps);
		log.debug("onNext");
		o.onNext((count));
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
