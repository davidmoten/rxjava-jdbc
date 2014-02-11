package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observer;

class QuerySelectRunnable<T> implements Runnable, Cancellable {

	private static final Logger log = Logger
			.getLogger(QuerySelectRunnable.class);

	private final Object connectionLock = new Object();
	private volatile Connection con;
	private volatile PreparedStatement ps;
	private volatile ResultSet rs;
	private final QuerySelect<T> query;
	private final List<Parameter> params;
	private final Observer<? super T> o;
	private final AtomicBoolean keepGoing = new AtomicBoolean(true);

	QuerySelectRunnable(QuerySelect<T> query, List<Parameter> params,
			Observer<? super T> o) {
		this.query = query;
		this.params = params;
		this.o = o;
	}

	@Override
	public void run() {
		try {

			log.debug(query.context().connectionProvider());
			synchronized (connectionLock) {
				if (keepGoing.get()) {
					con = query.context().connectionProvider().get();
					ps = con.prepareStatement(query.sql());
					Util.setParameters(ps, params);
				}
			}

			rs = ps.executeQuery();
			log.debug("executed ps=" + ps);
			while (keepGoing.get()) {
				synchronized (connectionLock) {
					if (rs.next()) {
						log.debug("onNext");
						o.onNext(query.function().call(rs));
					} else
						keepGoing.set(false);
				}
			}
			log.debug("onCompleted");
			o.onCompleted();
			synchronized (connectionLock) {
				close();
			}
		} catch (Exception e) {
			log.debug("onError: " + e.getMessage());
			o.onError(e);
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
			keepGoing.set(false);
			close();
		}
	}

}
