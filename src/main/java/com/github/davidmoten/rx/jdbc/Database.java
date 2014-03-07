package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Functions;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.RxUtil;

/**
 * Main entry point for manipulations of a database using rx-java-jdbc style
 * queries.
 */
final public class Database {

    /**
     * Optional query execution handlers for logging etc.
     */
    private final Handlers handlers;

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
     * Connection provider.
     */
    private final ConnectionProvider cp;

    /**
     * Schedules non transactional queries.
     */
    private final Func0<Scheduler> nonTransactionalSchedulerFactory;

    /**
     * Schedules transactional queries.
     */
    private final Func0<Scheduler> transactionalSchedulerFactory;

	/**
	 * Overrides query context for transactions. Null if no override.
	 */
	private final QueryContext contextOverride;

    /**
     * Constructor.
     * 
     * @param cp
     *            provides connections
     * @param selectHandler
     *            handles select queries
     * @param updateHandler
     *            handles update queries
     * @param nonTransactionalSchedulerFactory
     *            schedules non transactional queries
     * @param transactionalSchedulerFactory
     *            schedules transactional queries
     */
    public Database(final ConnectionProvider cp, Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler,
            Func1<Observable<Integer>, Observable<Integer>> updateHandler,
            Func0<Scheduler> nonTransactionalSchedulerFactory, Func0<Scheduler> transactionalSchedulerFactory, 
            QueryContext contextOverride) {
		Conditions.checkNotNull(cp);
        this.cp = cp;
        if (nonTransactionalSchedulerFactory != null)
            this.nonTransactionalSchedulerFactory = nonTransactionalSchedulerFactory;
        else
            this.nonTransactionalSchedulerFactory = IO_SCHEDULER_FACTORY;
        if (transactionalSchedulerFactory != null)
            this.transactionalSchedulerFactory = transactionalSchedulerFactory;
        else
            this.transactionalSchedulerFactory = SINGLE_THREAD_POOL_SCHEDULER_FACTORY;
        this.handlers = new Handlers(selectHandler, updateHandler);
        this.contextOverride = contextOverride;
    }

    private Database overrideContext(QueryContext context) {
    	return new Database(cp,handlers.selectHandler(), handlers.updateHandler(),
    			nonTransactionalSchedulerFactory, transactionalSchedulerFactory, context);
    }
    
    /**
     * Schedules on {@link Schedulers#io()}.
     */
    private final Func0<Scheduler> IO_SCHEDULER_FACTORY = new Func0<Scheduler>() {
        @Override
        public Scheduler call() {
            return Schedulers.io();
        }
    };

    /**
     * Schedules on a new single thread executor.
     */
    private final Func0<Scheduler> SINGLE_THREAD_POOL_SCHEDULER_FACTORY = new Func0<Scheduler>() {
        @Override
        public Scheduler call() {
            return Schedulers.executor(createSingleThreadExecutor());
        }
    };

    /**
     * Schedules using {@link Schedulers}.trampoline().
     */
    private static final Func0<Scheduler> CURRENT_THREAD_SCHEDULER_FACTORY = new Func0<Scheduler>() {

        @Override
        public Scheduler call() {
            return Schedulers.trampoline();
        }
    };

    /**
     * Constructor. Thread pool size defaults to
     * <code>{@link Runtime#getRuntime()}.availableProcessors()+1</code>. This
     * may be too conservative if the database is on another server. If that is
     * the case then you may want to use a thread pool size equal to the
     * available processors + 1 on the database server.
     * 
     * @param cp
     *            provides connections
     */
    public Database(ConnectionProvider cp) {
        this(cp, Functions.<Observable<ResultSet>> identity(), Functions.<Observable<Integer>> identity(), null, null,null);
    }

    /**
     * Constructor. Uses a {@link ConnectionProviderFromUrl} based on the given
     * url.
     * 
     * @param url
     *            jdbc connection url
     */
    public Database(String url) {
        this(new ConnectionProviderFromUrl(url));
    }

    /**
     * Returns a new {@link Builder}.
     * 
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds a {@link Database}.
     */
    public final static class Builder {

        private ConnectionProvider cp;
        private Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler = Functions.identity();
        private Func1<Observable<Integer>, Observable<Integer>> updateHandler = Functions.identity();
        private Func0<Scheduler> nonTransactionalSchedulerFactory = null;
        private Func0<Scheduler> transactionalSchedulerFactory = null;

        /**
         * Constructor.
         */
        private Builder() {
        }

        /**
         * Sets the connection provider.
         * 
         * @param cp
         * @return
         */
        public Builder connectionProvider(ConnectionProvider cp) {
            this.cp = cp;
            return this;
        }

        /**
         * Sets the jdbc url.
         * 
         * @param url
         * @return
         */
        public Builder url(String url) {
            this.cp = new ConnectionProviderFromUrl(url);
            return this;
        }

        /**
         * Sets the {@link ConnectionProvider} to use a connection pool with the
         * given jdbc url and pool size.
         * 
         * @param url
         * @param minPoolSize
         * @param maxPoolSize
         * @return
         */
        public Builder pooled(String url, int minPoolSize, int maxPoolSize) {
            this.cp = new ConnectionProviderPooled(url, minPoolSize, maxPoolSize);
            return this;
        }

        /**
         * Sets the {@link ConnectionProvider} to use a connection pool with the
         * given jdbc url and min pool size of 0, max pool size of 10.
         * 
         * @param url
         * @return
         */
        public Builder pooled(String url) {
            this.cp = new ConnectionProviderPooled(url, 0, 10);
            return this;
        }

        /**
         * Sets the select handler.
         * 
         * @param selectHandler
         * @return
         */
        public Builder selectHandler(Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler) {
            this.selectHandler = selectHandler;
            return this;
        }

        /**
         * Sets the update handler.
         * 
         * @param updateHandler
         * @return
         */
        public Builder updateHandler(Func1<Observable<Integer>, Observable<Integer>> updateHandler) {
            this.updateHandler = updateHandler;
            return this;
        }

        /**
         * Sets the non transactional scheduler.
         * 
         * @param factory
         * @return
         */
        public Builder nonTransactionalScheduler(Func0<Scheduler> factory) {
            nonTransactionalSchedulerFactory = factory;
            return this;
        }

        /**
         * Sets the transactional scheduler.
         * 
         * @param factory
         * @return
         */
        public Builder transactionalScheduler(Func0<Scheduler> factory) {
            transactionalSchedulerFactory = factory;
            return this;
        }

        /**
         * Requests that the non transactional queries are run using
         * {@link Schedulers#trampoline()}.
         * 
         * @return
         */
        public Builder nonTransactionalSchedulerOnCurrentThread() {
            nonTransactionalSchedulerFactory = CURRENT_THREAD_SCHEDULER_FACTORY;
            return this;
        }

        /**
         * Request that the transactional queries are run using
         * {@link Schedulers#trampoline()}.
         * 
         * @return
         */
        public Builder transactionalSchedulerOnCurrentThread() {
            transactionalSchedulerFactory = CURRENT_THREAD_SCHEDULER_FACTORY;
            return this;
        }

        /**
         * Sets both select and update handlers to the same handler.
         * 
         * @param handler
         * @return
         */
        public Builder handler(final Func1<Observable<Object>, Observable<Object>> handler) {
            this.selectHandler = new Func1<Observable<ResultSet>, Observable<ResultSet>>() {
                @Override
                public Observable<ResultSet> call(Observable<ResultSet> result) {
                    return handler.call(result.cast(Object.class)).cast(ResultSet.class);
                }
            };
            this.updateHandler = new Func1<Observable<Integer>, Observable<Integer>>() {
                @Override
                public Observable<Integer> call(Observable<Integer> result) {
                    return handler.call(result.cast(Object.class)).cast(Integer.class);
                }
            };
            return this;
        }

        /**
         * Returns a {@link Database}.
         * 
         * @return
         */
        public Database build() {
            return new Database(cp, selectHandler, updateHandler, transactionalSchedulerFactory,
                    nonTransactionalSchedulerFactory,null);
        }
    }

    /**
     * Returns the thread local current query context (will not return null). Will return overriden 
     * context (for example using Database returned from {@link Database#beginTransaction()} if set.
     * 
     * @return
     */
    public QueryContext queryContext() {
    	if (contextOverride!=null) 
    		return contextOverride;
    	
        if (context.get() == null) {
            context.set(QueryContext.newNonTransactionalQueryContext(cp, handlers,
                    nonTransactionalSchedulerFactory.call()));
        }
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
    	if (contextOverride!=null)
    		throw new RuntimeException("cannot begin transaction when query context is overriden (don't call this method on the Database return of db.beginTransaction()).");
        QueryContext queryContext = QueryContext.newTransactionalQueryContext(cp, handlers,
                transactionalSchedulerFactory.call());
        context.set(queryContext);
        return overrideContext(queryContext);
//        return this;
    }
    
    /**
     * Creates a {@link ScheduledExecutorService} based on a single thread pool.
     * 
     * @return executor service based on single thread pool.
     */
    private static ScheduledExecutorService createSingleThreadExecutor() {
        return Executors.newScheduledThreadPool(1, new ThreadFactory() {
            final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "TransactionThreadPool-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
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
    
    //TODO get this working?
    private <T> Operator<Boolean,T> commitOperator() {
    	return RxUtil.toOperator(new Func1<Observable<T>,Observable<Boolean>>() {

			@Override
			public Observable<Boolean> call(Observable<T> depends) {
				return commit(depends);
			}});
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
    private Observable<Boolean> commitOrRollback(boolean commit, Observable<?>... depends) {
    	
    	if (contextOverride!=null)
    		throw new RuntimeException("cannot commit or rollback when query context is override (don't call this method on the result of db.beginTransaction()).");
        String action;
        if (commit)
            action = "commit";
        else
            action = "rollback";
        QueryUpdate.Builder u = update(action);
        for (Observable<?> dep : depends)
            u = u.dependsOn(dep);
        Observable<Boolean> result = u.count().map(IS_NON_ZERO);
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
    public Observable<Boolean> lastTransactionResult() {
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
        context.set(null);
    }

    /**
     * Close the database in particular closes the {@link ConnectionProvider}
     * for the database. For a {@link ConnectionProviderPooled} this will be a
     * required call for cleanup.
     * 
     * @return
     */
    public Database close() {
        cp.close();
        return this;
    }

}