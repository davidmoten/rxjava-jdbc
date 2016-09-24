package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.RxUtil.greaterThanZero;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import javax.naming.Context;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.rx.Functions;
import com.github.davidmoten.rx.RxUtil;
import com.github.davidmoten.rx.RxUtil.CountingAction;
import com.github.davidmoten.rx.jdbc.exceptions.TransactionAlreadyOpenException;

import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.StringObservable;
import rx.schedulers.Schedulers;

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

    static final ThreadLocal<ResultSetCache> rsCache = new ThreadLocal<ResultSetCache>();

    static final ThreadLocal<AutoMapCache> autoMapCache = new ThreadLocal<AutoMapCache>();

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
     * ResultSets are transformed with this transform once on creation in select
     * queries
     */
    private final Func1<ResultSet, ? extends ResultSet> resultSetTransform;

    /**
     * Constructor.
     * 
     * @param cp
     *            provides connections
     * @param nonTransactionalSchedulerFactory
     *            schedules non transactional queries
     */
    public Database(final ConnectionProvider cp,
            Func0<Scheduler> nonTransactionalSchedulerFactory) {
        this(cp, nonTransactionalSchedulerFactory, IDENTITY_TRANSFORM);
    }

    private static Func1<ResultSet, ? extends ResultSet> IDENTITY_TRANSFORM = Functions.identity();

    /**
     * Constructor.
     * 
     * @param cp
     *            provides connections
     * @param nonTransactionalSchedulerFactory
     *            schedules non transactional queries
     * @param resultSetTransform
     *            transforms ResultSets at start of select query
     */
    public Database(final ConnectionProvider cp, Func0<Scheduler> nonTransactionalSchedulerFactory,
            Func1<ResultSet, ? extends ResultSet> resultSetTransform) {
        Conditions.checkNotNull(cp);
        this.cp = cp;
        currentConnectionProvider.set(cp);
        if (nonTransactionalSchedulerFactory != null)
            this.nonTransactionalSchedulerFactory = nonTransactionalSchedulerFactory;
        else
            this.nonTransactionalSchedulerFactory = CURRENT_THREAD_SCHEDULER_FACTORY;
        this.context = new QueryContext(this);
        this.resultSetTransform = resultSetTransform;
    }

    /**
     * Returns the currently defined {@link ResultSet} transform.
     * 
     * @return the current ResultSet transform applied at the start of select
     *         queries
     */
    public Func1<ResultSet, ? extends ResultSet> getResultSetTransform() {
        return resultSetTransform;
    }

    /**
     * Returns the {@link ConnectionProvider}.
     * 
     * @return
     */
    public ConnectionProvider getConnectionProvider() {
        return cp;
    }

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
     *            jdbc url
     * @param username
     *            username for connection
     * @param password
     *            password for connection
     */
    public Database(String url, String username, String password) {
        this(new ConnectionProviderFromUrl(url, username, password));
    }

    /**
     * Constructor. Uses the single connection provided and current thread
     * scheduler (trampoline) to run all queries. The connection will not be
     * closed in reality though the log may indicate it as having received a
     * close call.
     * 
     * @param con
     *            the connection
     */
    public Database(Connection con) {
        this(new ConnectionProviderNonClosing(con), CURRENT_THREAD_SCHEDULER_FACTORY);
    }

    /**
     * Returns a {@link Database} based on a jdbc connection string.
     * 
     * @param url
     *            jdbc connection url
     * @return
     */
    public static Database from(String url) {
        return new Database(url, null, null);
    }

    /**
     * Returns a {@link Database} based on a jdbc connection string.
     * 
     * @param url
     *            jdbc url
     * @param username
     *            username for connection
     * @param password
     *            password for connection
     * @return the database object
     */
    public static Database from(String url, String username, String password) {
        return new Database(url, username, password);
    }

    /**
     * Returns a {@link Database} based on connections obtained from a
     * {@link DataSource} based on looking up the current {@link Context}
     * 
     * @param jndiResource
     * @return
     */
    public static Database fromContext(String jndiResource) {
        return new Database(new ConnectionProviderFromContext(jndiResource));
    }

    /**
     * Returns a {@link Database} based on connections obtained from a
     * {@link DataSource}
     * 
     * @param jndiResource
     * @return
     */
    public static Database fromDataSource(DataSource dataSource) {
        return new Database(new ConnectionProviderFromDataSource(dataSource));
    }

    /**
     * Returns a {@link Database} that obtains {@link Connection}s on demand
     * from the given {@link ConnectionProvider}. When {@link Database#close()}
     * is called, {@link ConnectionProvider#close()} is called.
     * 
     * @param cp
     * @return
     */
    public static Database from(ConnectionProvider cp) {
        return new Database(cp);
    }

    /**
     * Factory method. Uses the single connection provided and current thread
     * scheduler (trampoline) to run all queries. The connection will not be
     * closed in reality though the log may indicate it as having received a
     * close call.
     * 
     * @param con
     *            the connection
     */
    public static Database from(Connection con) {
        return new Database(con);
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
        private Pool pool = null;
        private String url;
        private String username;
        private String password;
        private Func1<ResultSet, ? extends ResultSet> resultSetTransform = IDENTITY_TRANSFORM;

        private static class Pool {
            int minSize;
            int maxSize;

            Pool(int minSize, int maxSize) {
                super();
                this.minSize = minSize;
                this.maxSize = maxSize;
            }
        }

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
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
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
        public Builder pool(int minPoolSize, int maxPoolSize) {
            pool = new Pool(minPoolSize, maxPoolSize);
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
         * @return this
         */
        public Builder nonTransactionalSchedulerOnCurrentThread() {
            nonTransactionalSchedulerFactory = CURRENT_THREAD_SCHEDULER_FACTORY;
            return this;
        }

        /**
         * When a ResultSet is obtained by {@link Database#select()} Observable
         * then before being used it is transformed by the {@code transform}
         * function.
         * 
         * @param transform
         *            function that transforms the ResultSet into another
         *            ResultSet
         * @return this
         */
        public Builder resultSetTransform(Func1<ResultSet, ? extends ResultSet> transform) {
            this.resultSetTransform = transform;
            return this;
        }

        /**
         * Returns a {@link Database}.
         * 
         * @return the constructed Database
         */
        public Database build() {
            if (url != null && pool != null)
                cp = new ConnectionProviderPooled(url, username, password, pool.minSize,
                        pool.maxSize);
            else if (url != null)
                cp = new ConnectionProviderFromUrl(url, username, password);
            return new Database(cp, nonTransactionalSchedulerFactory, resultSetTransform);
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
     * Returns a {@link QuerySelect.Builder} builder and defers specifying sql
     * to the `autoMap` Class parameter.
     * 
     * @return query builder
     */
    public QuerySelect.Builder select() {
        return new QuerySelect.Builder(null, this);
    }

    /**
     * Returns a {@link QueryUpdate.Builder} builder based on the given
     * update/insert/delete/DDL statement sql.
     * 
     * @param sql
     *            an update/insert/delete/DDL statement.
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
        return update("begin").dependsOn(dependency).count().map(Functions.constant(true));
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

    /**
     * Returns the current thread local {@link Scheduler}.
     * 
     * @return
     */
    Scheduler currentScheduler() {
        if (currentSchedulerFactory.get() == null)
            return nonTransactionalSchedulerFactory.call();
        else
            return currentSchedulerFactory.get().call();
    }

    /**
     * Returns the current thread local {@link ConnectionProvider}.
     * 
     * @return
     */
    ConnectionProvider connectionProvider() {
        if (currentConnectionProvider.get() == null)
            return cp;
        else
            return currentConnectionProvider.get();
    }

    /**
     * Sets the current thread local {@link ConnectionProvider} to a singleton
     * manual commit instance.
     */
    void beginTransactionObserve() {
        log.debug("beginTransactionObserve");
        currentConnectionProvider.set(new ConnectionProviderSingletonManualCommitStatementCaching(cp));
        if (isTransactionOpen.get() != null && isTransactionOpen.get())
            throw new TransactionAlreadyOpenException();
        isTransactionOpen.set(true);
    }

    /**
     * Sets the current thread local {@link Scheduler} to be
     * {@link Schedulers#trampoline()}.
     */
    void beginTransactionSubscribe() {
        log.debug("beginTransactionSubscribe");
        currentSchedulerFactory.set(CURRENT_THREAD_SCHEDULER_FACTORY);
    }

    /**
     * Resets the current thread local {@link Scheduler} to default.
     */
    void endTransactionSubscribe() {
        log.debug("endTransactionSubscribe");
        currentSchedulerFactory.set(null);
        rsCache.set(null);
    }

    /**
     * Resets the current thread local {@link ConnectionProvider} to default.
     */
    void endTransactionObserve() {
        log.debug("endTransactionObserve");
        currentConnectionProvider.set(cp);
        isTransactionOpen.set(false);
        rsCache.set(null);
    }

    /**
     * Returns an {@link Operator} that performs commit or rollback of a
     * transaction.
     * 
     * @param isCommit
     * @return
     */
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
        return RxUtil.toOperator(new Func1<Observable<Observable<T>>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<Observable<T>> source) {
                return source.concatMap(new Func1<Observable<T>, Observable<Boolean>>() {
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
    public Operator<Boolean, ?> rollbackOnNextOperator() {
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

    private static <T> Observable<Boolean> commitOrRollbackOnCompleteOperatorIfAtLeastOneValue(
            final boolean isCommit, final Database db, Observable<T> source) {
        CountingAction<T> counter = RxUtil.counter();
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
    private static <T> Observable<Boolean> commitOrRollbackOnNext(final boolean isCommit,
            final Database db, Observable<T> source) {
        return source.concatMap(new Func1<T, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(T t) {
                if (isCommit)
                    return db.commit();
                else
                    return db.rollback();
            }
        });
    }

    private static <T> Observable<T> beginTransactionOnNext(final Database db,
            Observable<T> source) {
        return source.concatMap(new Func1<T, Observable<T>>() {
            @Override
            public Observable<T> call(T t) {
                return db.beginTransaction().map(Functions.constant(t));
            }
        });
    }

    /**
     * Returns an {@link Observable} that is the result of running a sequence of
     * update commands (insert/update/delete, ddl) read from the given
     * {@link Observable} sequence.
     * 
     * @param commands
     * @return
     */
    public Observable<Integer> run(Observable<String> commands) {
        return commands.reduce(Observable.<Integer> empty(),
                new Func2<Observable<Integer>, String, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(Observable<Integer> dep, String command) {
                        return update(command).dependsOn(dep).count();
                    }
                }).lift(RxUtil.<Integer> flatten());
    }

    /**
     * Returns an {@link Operator} version of {@link #run(Observable)}.
     * 
     * @return
     */
    public Operator<Integer, String> run() {
        return RxUtil.toOperator(new Func1<Observable<String>, Observable<Integer>>() {
            @Override
            public Observable<Integer> call(Observable<String> commands) {
                return run(commands);
            }
        });
    }

    /**
     * Returns an {@link Observable} that is the result of running a sequence of
     * update commands (insert/update/delete, ddl) commands read from an
     * InputStream using the given delimiter as the statement delimiter (for
     * example semicolon).
     * 
     * @param is
     * @param delimiter
     * @return
     */
    public Observable<Integer> run(InputStream is, String delimiter) {
        return run(is, Charset.defaultCharset(), delimiter);
    }

    /**
     * Returns an {@link Observable} that is the result of running a sequence of
     * update commands (insert/update/delete, ddl) commands read from an
     * {@link InputStream} with the given {@link Charset} using the given
     * delimiter as the statement delimiter (for example semicolon).
     * 
     * @param is
     * @param delimiter
     * @return
     */
    public Observable<Integer> run(InputStream is, Charset charset, String delimiter) {
        return StringObservable
                .split(StringObservable.from(new InputStreamReader(is, charset)), ";").lift(run());
    }

    /**
     * Returns a Database based on the current Database except all
     * non-transactional queries run {@link Schedulers#io}.
     * 
     * @return new Database instance
     */
    public Database asynchronous() {
        return asynchronous(Schedulers.io());
    }

    /**
     * Returns a Database based on the current Database except all
     * non-transactional queries run on the given scheduler.
     * 
     * @return new Database instance
     */
    public Database asynchronous(final Scheduler nonTransactionalScheduler) {
        return asynchronous(new Func0<Scheduler>() {
            @Override
            public Scheduler call() {
                return nonTransactionalScheduler;
            }
        });
    }

    /**
     * Returns a Database based on the current Database except all
     * non-transactional queries run on the scheduler provided by the given
     * factory.
     * 
     * @return new Database instance
     */
    public Database asynchronous(final Func0<Scheduler> nonTransactionalSchedulerFactory) {
        return new Database(cp, nonTransactionalSchedulerFactory);
    }

    /**
     * Sentinel object used to indicate in parameters of a query that rather
     * than calling {@link PreparedStatement#setObject(int, Object)} with a null
     * we call {@link PreparedStatement#setNull(int, int)} with
     * {@link Types#CLOB}. This is required by many databases for setting CLOB
     * and BLOB fields to null.
     */
    public static final Object NULL_CLOB = new Object();

    public static final Object NULL_NUMBER = new Object();

    public static Object toSentinelIfNull(String s) {
        if (s == null)
            return NULL_CLOB;
        else
            return s;
    }

    /**
     * Sentinel object used to indicate in parameters of a query that rather
     * than calling {@link PreparedStatement#setObject(int, Object)} with a null
     * we call {@link PreparedStatement#setNull(int, int)} with
     * {@link Types#CLOB}. This is required by many databases for setting CLOB
     * and BLOB fields to null.
     */
    public static final Object NULL_BLOB = new Object();

    public static Object toSentinelIfNull(byte[] bytes) {
        if (bytes == null)
            return NULL_BLOB;
        else
            return bytes;
    }

}