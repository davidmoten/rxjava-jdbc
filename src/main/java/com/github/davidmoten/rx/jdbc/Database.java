package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.RxUtil.constant;
import static com.github.davidmoten.rx.RxUtil.greaterThanZero;

import java.sql.Connection;

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

    /**
     * Logger.
     */
    private static final Logger log = LoggerFactory.getLogger(Database.class);

    /**
     * Provides access for queries to a limited subset of {@link Database}
     * methods.
     */
    private final QueryContext context;

    /**
     * ThreadLocal storage of the current {@link Scheduler} factory to use with
     * queries.
     */
    private final ThreadLocal<Func0<Scheduler>> currentSchedulerFactory = new ThreadLocal<Func0<Scheduler>>();

    /**
     * ThreadLocal storage of the current {@link ConnectionProvider} to use with
     * queries.
     */
    private final ThreadLocal<ConnectionProvider> currentConnectionProvider = new ThreadLocal<ConnectionProvider>();

    private final ThreadLocal<Boolean> isTransactionOpen = new ThreadLocal<Boolean>();

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
     * @param nonTransactionalSchedulerFactory
     *            schedules non transactional queries
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
        isTransactionOpen.set(false);
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

    /**
     * Waits for the source to complete before returning the result of
     * db.commit();
     * 
     * @return commit operator
     */
    public <T> Operator<Boolean, T> commitOperator() {
        return commitOrRollbackOperator(true);
    }

    /**
     * Waits for the source to complete before returning the result of
     * db.rollback();
     * 
     * @return rollback operator
     */
    public <T> Operator<Boolean, T> rollbackOperator() {
        return commitOrRollbackOperator(false);
    }

    private <T> Operator<Boolean, T> commitOrRollbackOperator(final boolean commit) {
        final QueryUpdate.Builder updateBuilder = createCommitOrRollbackQuery(commit);
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> source) {
                return updateBuilder.dependsOn(source).count().map(IS_NON_ZERO);
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
        log.debug("closing connection provider");
        cp.close();
        log.debug("closed connection provider");
        return this;
    }

    Scheduler currentScheduler() {
        if (currentSchedulerFactory.get() == null)
            return nonTransactionalSchedulerFactory.call();
        else
            return currentSchedulerFactory.get().call();
    }

    ConnectionProvider connectionProvider() {
        if (currentConnectionProvider.get() == null)
            return cp;
        else
            return currentConnectionProvider.get();
    }

    void beginTransactionObserve() {
        log.debug("beginTransactionObserve");
        currentConnectionProvider.set(new ConnectionProviderSingletonManualCommit(cp));
        if (isTransactionOpen.get())
            throw new RuntimeException("cannot begin transaction as transaction open already");
        isTransactionOpen.set(true);
    }

    void beginTransactionSubscribe() {
        log.debug("beginTransactionSubscribe");
        currentSchedulerFactory.set(CURRENT_THREAD_SCHEDULER_FACTORY);
    }

    void endTransactionSubscribe() {
        log.debug("endTransactionSubscribe");
        currentSchedulerFactory.set(null);
    }

    void endTransactionObserve() {
        log.debug("endTransactionObserve");
        currentConnectionProvider.set(cp);
        isTransactionOpen.set(false);
    }

    private <T> Operator<Boolean, T> commitOrRollbackOnCompleteOperator(final boolean isCommit) {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> source) {
                return commitOrRollbackOnCompleteOperatorIfAtLeastOneValue(isCommit, Database.this,
                        source);
            }
        });
    }

    /**
     * Commits current transaction on the completion of source if and only if
     * the source sequence is non-empty.
     * 
     * @return operator that commits on completion of source.
     */
    public <T> Operator<Boolean, T> commitOnCompleteOperator() {
        return commitOrRollbackOnCompleteOperator(true);
    }

    /**
     * Rolls back current transaction on the completion of source if and only if
     * the source sequence is non-empty.
     * 
     * @return operator that rolls back on completion of source.
     */

    public <T> Operator<Boolean, T> rollbackOnCompleteOperator() {
        return commitOrRollbackOnCompleteOperator(false);
    }

    /**
     * Starts a database transaction for each onNext call. Following database
     * calls will be subscribed on current thread (Schedulers.trampoline()) and
     * share the same {@link Connection} until transaction is rolled back or
     * committed.
     * 
     * @return begin transaction operator
     */
    public <T> Operator<T, T> beginTransactionOnNextOperator() {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<T>>() {
            @Override
            public Observable<T> call(Observable<T> source) {
                return beginTransactionOnNext(Database.this, source);
            }
        });
    }

    /**
     * Commits the currently open transaction. Emits true.
     * 
     * @return
     */
    public <T> Operator<Boolean, T> commitOnNextOperator() {
        return commitOrRollbackOnNextOperator(true);
    }

    public <T> Operator<Boolean, Observable<T>> commitOnNextListOperator() {
        return commitOrRollbackOnNextListOperator(true);
    }
    
    public <T> Operator<Boolean, Observable<T>> rollbackOnNextListOperator() {
        return commitOrRollbackOnNextListOperator(false);
    }

    private <T> Operator<Boolean, Observable<T>> commitOrRollbackOnNextListOperator(
            final boolean isCommit) {
        return RxUtil
                .toOperator(new Func1<Observable<Observable<T>>, Observable<Boolean>>() {
                    @Override
                    public Observable<Boolean> call(Observable<Observable<T>> source) {
                        return source.flatMap(new Func1<Observable<T>, Observable<Boolean>>() {
                            @Override
                            public Observable<Boolean> call(Observable<T> source) {
                                if (isCommit)
                                    return commit(source);
                                else
                                    return rollback(source);
                            }
                        });
                    }
                });
    }

    /**
     * Rolls back the current transaction. Emits false.
     * 
     * @return
     */
    public <T> Operator<Boolean, T> rollbackOnNextOperator() {
        return commitOrRollbackOnNextOperator(false);
    }

    private <T> Operator<Boolean, T> commitOrRollbackOnNextOperator(final boolean isCommit) {
        return RxUtil.toOperator(new Func1<Observable<T>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<T> source) {
                return commitOrRollbackOnNext(isCommit, Database.this, source);
            }
        });
    }

    private static final <T> Observable<Boolean> commitOrRollbackOnCompleteOperatorIfAtLeastOneValue(
            final boolean isCommit, final Database db, Observable<T> source) {
        CounterAction<T> counter = RxUtil.counter();
        Observable<Boolean> commit = counter
        // get count
                .count()
                // greater than zero or empty
                .filter(greaterThanZero())
                // commit if at least one value
                .lift(db.commitOrRollbackOperator(isCommit));
        return Observable
        // concatenate
                .concat(source
                // count emissions
                        .doOnNext(counter)
                        // ignore emissions
                        .ignoreElements()
                        // cast the empty sequence to type Boolean
                        .cast(Boolean.class),
                // concat with commit
                        commit);
    }

    /**
     * Emits true for commit and false for rollback.
     * 
     * @param isCommit
     * @param db
     * @param source
     * @return
     */
    private static final <T> Observable<Boolean> commitOrRollbackOnNext(final boolean isCommit,
            final Database db, Observable<T> source) {
        return source.flatMap(new Func1<T, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(T t) {
                if (isCommit)
                    return db.commit();
                else
                    return db.rollback();
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