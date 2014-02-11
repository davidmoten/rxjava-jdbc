package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.concurrent.Executors;

import rx.Observable;
import rx.util.functions.Func1;

import com.github.davidmoten.rx.jdbc.connection.AutoCommittingConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.connection.SimpleConnectionProvider;

/**
 * Main entry point for manipulations of a database using rx-java-jdbc style
 * queries.
 */
public class Database {

	private static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime()
			.availableProcessors() + 1;

	/**
	 * Provides {@link Connection}s for queries.
	 */
	private final ConnectionProvider cp;

	/**
	 * Records the current query context which will be set against a new query
	 * at create time (not at runtime).
	 */
	private final ThreadLocal<QueryContext> context = new ThreadLocal<QueryContext>();

	/**
	 * Records the result of the last finished transaction (committed =
	 * <code>true</code> or rolled back = <code>false</code>).
	 */
	private final ThreadLocal<Observable<Boolean>> lastTransactionResult = new ThreadLocal<Observable<Boolean>>();

	/**
	 * Used to run queries in autocommit mode outside of transactions.
	 */
	private final QueryContext asynchronousQueryContext;

	/**
	 * Constructor.
	 * 
	 * @param cp
	 *            provides connections
	 * @param threadPoolSize
	 *            for asynchronous query context
	 */
	public Database(ConnectionProvider cp, int threadPoolSize) {
		this.cp = cp;
		this.asynchronousQueryContext = new QueryContext(
				Executors.newFixedThreadPool(threadPoolSize),
				new AutoCommittingConnectionProvider(cp));
	}

	/**
	 * Constructor. Thread pool size defaults to
	 * <code>{@link Runtime}.getRuntime().availableProcessors()+1</code>. This
	 * may be too conservative if the database is on another server. If that is
	 * the case then you may want to use a thread pool size equal to the
	 * available processors + 1 on the database server.
	 * 
	 * @param cp
	 *            provides connections
	 */
	public Database(ConnectionProvider cp) {
		this(cp, DEFAULT_THREAD_POOL_SIZE);
	}

	/**
	 * Constructor. Uses a {@link SimpleConnectionProvider} based on the given
	 * url.
	 * 
	 * @param url
	 *            jdbc connection url
	 */
	public Database(String url) {
		this(new SimpleConnectionProvider(url));
	}

	/**
	 * Returns the thread local current query context (will not return null).
	 * 
	 * @return
	 */
	public QueryContext getQueryContext() {
		if (context.get() == null)
			return asynchronousQueryContext;
		else
			return context.get();
	}

	/**
	 * Returns a {@link QuerySelect.Builder} builder based on the given select
	 * statement sql.
	 * 
	 * @param sql
	 *            a select statement.
	 * @return select query builder
	 */
	public QuerySelect.Builder select(String sql) {
		return new QuerySelect.Builder(sql, this);
	}

	/**
	 * Returns a {@link QueryUpdate.Builder} builder based on the given
	 * update/insert statement sql.
	 * 
	 * @param sql
	 *            an update or insert statement.
	 * @return update/insert query builder
	 */
	public QueryUpdate.Builder update(String sql) {
		return new QueryUpdate.Builder(sql, this);
	}

	/**
	 * Starts a transaction. Until commit() or rollback() is called on the
	 * source this will set the query context for all created queries to be a
	 * single threaded executor with one (new) connection.
	 * 
	 * @return
	 */
	public Database beginTransaction() {
		context.set(new TransactionalQueryContext(cp));
		return this;
	}

	/**
	 * Returns true if and only if integer is non-zero.
	 */
	private static final Func1<Integer, Boolean> IS_NON_ZERO = new Func1<Integer, Boolean>() {
		@Override
		public Boolean call(Integer i) {
			return i != 0;
		}
	};

	/**
	 * Commits a transaction and resets the current query context so that
	 * further queries will use the asynchronous version by default. All
	 * Observable dependencies must be complete before commit is called.
	 * 
	 * @param depends
	 *            depdencies that must complete before commit occurs.
	 * @return
	 */
	public Observable<Boolean> commit(Observable<?>... depends) {
		return commitOrRollback(true, depends);
	}

	/**
	 * Commits or rolls back a transaction depending on the <code>commit</code>
	 * parameter and resets the current query context so that further queries
	 * will use the asynchronous version by default. All Observable dependencies
	 * must be complete before commit/rollback is called.
	 * 
	 * @param commit
	 * @param depends
	 * @return
	 */
	private Observable<Boolean> commitOrRollback(boolean commit,
			Observable<?>... depends) {
		String action;
		if (commit)
			action = "commit";
		else
			action = "rollback";
		QueryUpdate.Builder u = update(action);
		for (Observable<?> dep : depends)
			u = u.dependsOn(dep);
		Observable<Boolean> result = u.getCount().map(IS_NON_ZERO);
		lastTransactionResult.set(result);
		resetQueryContext();
		return result;
	}

	/**
	 * Rolls back a transaction and resets the current query context so that
	 * further queries will use the asynchronous version by default. All
	 * Observable dependencies must be complete before rollback is called.
	 * 
	 * @param depends
	 *            depdencies that must complete before commit occurs.
	 * @return
	 * 
	 **/
	public Observable<Boolean> rollback(Observable<?>... depends) {
		return commitOrRollback(false, depends);
	}

	/**
	 * Returns observable that emits true when last transaction committed or
	 * false when last transaction is rolled back.
	 * 
	 * @return
	 */
	public Observable<Boolean> getLastTransactionResult() {
		Observable<Boolean> o = lastTransactionResult.get();
		if (o == null)
			return Observable.empty();
		else
			return o;
	}

	/**
	 * Revert query context to default asynchronous version.
	 */
	private void resetQueryContext() {
		context.set(asynchronousQueryContext);
	}

}