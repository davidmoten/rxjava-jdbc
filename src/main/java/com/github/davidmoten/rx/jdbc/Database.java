package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.RxUtil.constant;
import static com.github.davidmoten.rx.RxUtil.greaterThanZero;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.RxUtil;
import com.github.davidmoten.rx.RxUtil.CounterAction;

/**
 * Main entry point for manipulations of a database using rx-java-jdbc style
 * queries.
 */
final public class Database {

    private static final Logger log = LoggerFactory.getLogger(Database.class);

    /**
     * Records the current query context which will be set against a new query
     * at create time (not at runtime).
     */
    private final QueryContext context;

    private final ThreadLocal<Func0<Scheduler>> currentSchedulerFactory = new ThreadLocal<Func0<Scheduler>>();

    private final ThreadLocal<ConnectionProvider> currentConnectionProvider = new ThreadLocal<ConnectionProvider>();

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
    public Database(final ConnectionProvider cp, Func0<Scheduler> nonTransactionalSchedulerFactory) {
        Conditions.checkNotNull(cp);
        this.cp = cp;
        currentConnectionProvider.set(cp);
        if (nonTransactionalSchedulerFactory != null)
            this.nonTransactionalSchedulerFactory = nonTransactionalSchedulerFactory;
        else
            this.nonTransactionalSchedulerFactory = IO_SCHEDULER_FACTORY;
        this.context = new QueryContext(this);
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

    private final AtomicBoolean currentIsTransactionOpen = new AtomicBoolean(false);

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
        this(cp, null);
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
        private Func0<Scheduler> nonTransactionalSchedulerFactory = null;

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
         * Returns a {@link Database}.
         * 
         * @return
         */
        public Database build() {
            return new Database(cp, nonTransactionalSchedulerFactory);
        }
    }

    /**
     * Returns the thread local current query context (will not return null).
     * Will return overriden context (for example using Database returned from
     * {@link Database#beginTransaction()} if set.
     * 
     * @return
     */
    public QueryContext queryContext() {
        return context;
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
     * @param dependency
     * @return
     */
    public Observable<Boolean> beginTransaction(Observable<?> dependency) {
        return update("begin").dependsOn(dependency).count().map(IS_NON_ZERO);
    }

    /**
     * Starts a transaction. Until commit() or rollback() is called on the
     * source this will set the query context for all created queries to be a
     * single threaded executor with one (new) connection.
     * 
     * @return
     */
    public Observable<Boolean> beginTransaction() {
        return beginTransaction(Observable.empty());
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

    public <T> Operator<Boolean, T> commitOperator() {
        return commitOrRollbackOperator(true);
    }

    public <T> Operator<Boolean, T> rollbackOperator() {
        return commitOrRollbackOperator(false);
    }

    private <T> Operator<Boolean, T> commitOrRollbackOperator(final boolean commit) {
        final QueryUpdate.Builder updateBuilder = createCommitOrRollbackQuery(commit);
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> dependency) {
                return updateBuilder.dependsOn(dependency).count().map(IS_NON_ZERO);
            }
        });
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

        QueryUpdate.Builder u = createCommitOrRollbackQuery(commit);
        for (Observable<?> dep : depends)
            u = u.dependsOn(dep);
        Observable<Boolean> result = u.count().map(IS_NON_ZERO);
        lastTransactionResult.set(result);
        return result;
    }

    private QueryUpdate.Builder createCommitOrRollbackQuery(boolean commit) {
        String action;
        if (commit)
            action = "commit";
        else
            action = "rollback";
        QueryUpdate.Builder u = update(action);
        return u;
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

    public Scheduler currentScheduler() {
        if (currentSchedulerFactory.get() == null)
            return nonTransactionalSchedulerFactory.call();
        else
            return currentSchedulerFactory.get().call();
    }

    public ConnectionProvider connectionProvider() {
        if (currentConnectionProvider.get() == null)
            return cp;
        else
            return currentConnectionProvider.get();
    }

    void beginTransactionObserve() {
        log.debug("beginTransactionObserve");
        currentConnectionProvider.set(new ConnectionProviderSingletonManualCommit(cp));
        currentIsTransactionOpen.set(true);
    }

    void beginTransactionSubscribe() {
        log.debug("beginTransactionSubscribe");
        currentSchedulerFactory.set(CURRENT_THREAD_SCHEDULER_FACTORY);
        currentIsTransactionOpen.set(true);
    }

    public void endTransactionSubscribe() {
        log.debug("endTransactionSubscribe");
        currentSchedulerFactory.set(null);
        currentIsTransactionOpen.set(false);
    }

    public void endTransactionObserve() {
        log.debug("endTransactionObserve");
        currentConnectionProvider.set(cp);
        currentIsTransactionOpen.set(false);
    }

    public boolean transactionIsOpen() {
        return currentIsTransactionOpen.get();
    }

    public <T> Operator<Boolean, T> commitOnCompleteOperator() {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> source) {
                return commitOnCompleteOperatorIfAtLeastOneValue(Database.this, source);
            }
        });
    }

    public <T> Operator<T, T> beginTransactionOnNextOperator() {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<T>>() {
            @Override
            public Observable<T> call(Observable<T> source) {
                return beginTransactionOnNext(Database.this, source);
            }
        });
    }

    public <T> Operator<Boolean, T> commitOnNextOperator() {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> source) {
                return commitOnNext(Database.this, source);
            }
        });
    }

    private static final <T> Observable<Boolean> commitOnCompleteOperatorIfAtLeastOneValue(final Database db,
            Observable<T> source) {
        CounterAction<T> counter = RxUtil.counter();
        Observable<Boolean> commit = counter
        		//get count
        		.count()
        		//greater than zero or empty
        		.filter(greaterThanZero())
        		//commit if at least one value
        		.lift(db.commitOperator());
        return Observable
        		//concatenate
        		.concat(source
        				//count emissions
        				.doOnNext(counter)
        				//ignore emissions
        				.ignoreElements()
        				//cast the empty sequence to type Boolean
        				.cast(Boolean.class),
        				//concat with commit
        				commit);
    }

    private static final <T> Observable<Boolean> commitOnNext(final Database db, Observable<T> source) {
        return source.flatMap(new Func1<T, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(T t) {
                return db.commit();
            }
        });
    }

    private static <T> Observable<T> beginTransactionOnNext(final Database db, Observable<T> source) {
        return source.flatMap(new Func1<T, Observable<T>>() {
            @Override
            public Observable<T> call(T t) {
                return db.beginTransaction().map(constant(t));
            }
        });
    }
}