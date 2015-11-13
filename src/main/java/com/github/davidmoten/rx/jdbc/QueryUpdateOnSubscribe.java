package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.Exceptions;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

/**
 * Executes the update query.
 */
final class QueryUpdateOnSubscribe<T> implements OnSubscribe<T> {

    private static final Logger log = LoggerFactory.getLogger(QueryUpdateOnSubscribe.class);

    static final String BEGIN_TRANSACTION = "begin";

    /**
     * Special sql command that brings about a rollback.
     */
    static final String ROLLBACK = "rollback";

    /**
     * Special sql command that brings about a commit.
     */
    static final String COMMIT = "commit";

    /**
     * Returns an Observable of the results of pushing one set of parameters
     * through a select query.
     * 
     * @param params
     *            one set of parameters to be run with the query
     * @return
     */
    static <T> Observable<T> execute(QueryUpdate<T> query, List<Parameter> parameters,
            AtomicInteger batchCounter) {
        return Observable.create(new QueryUpdateOnSubscribe<T>(query, parameters, batchCounter));
    }

    /**
     * The query to be executed.
     */
    private final QueryUpdate<T> query;

    /**
     * The parameters to run the query against (may be a subset of the query
     * parameters specified in the query because the query may be run multiple
     * times with multiple sets of parameters).
     */
    private final List<Parameter> parameters;

    private final AtomicInteger batchCounter;

    /**
     * Constructor.
     * 
     * @param query
     * @param parameters
     */
    private QueryUpdateOnSubscribe(QueryUpdate<T> query, List<Parameter> parameters,
            AtomicInteger batchCounter) {
        this.query = query;
        this.parameters = parameters;
        this.batchCounter = batchCounter;
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {
        final State state = new State();
        try {
            if (isBeginTransaction())
                performBeginTransaction(subscriber);
            else {
                getConnection(state);
                subscriber.add(createUnsubscriptionAction(state));
                if (isCommit())
                    performCommit(subscriber, state, batchCounter);
                else if (isRollback())
                    performRollback(subscriber, state);
                else
                    performUpdate(subscriber, state, batchCounter);
            }
        } catch (Throwable e) {
            query.context().endTransactionObserve();
            query.context().endTransactionSubscribe();
            try {
                close(state);
            } finally {
                handleException(e, subscriber);
            }
        }
    }

    private Subscription createUnsubscriptionAction(final State state) {
        return Subscriptions.create(new Action0() {
            @Override
            public void call() {
                close(state);
            }
        });
    }

    private boolean isBeginTransaction() {
        return query.sql().equals(BEGIN_TRANSACTION);
    }

    @SuppressWarnings("unchecked")
    private void performBeginTransaction(Subscriber<? super T> subscriber) {
        query.context().beginTransactionObserve();
        log.debug("beginTransaction emitting 1");
        subscriber.onNext((T) Integer.valueOf(1));
        log.debug("emitted 1");
        complete(subscriber);
    }

    /**
     * Gets the current connection.
     */
    private void getConnection(State state) {
        log.debug("getting connection");
        state.con = query.context().connectionProvider().get();
        log.debug("cp={}", query.context().connectionProvider());
    }

    /**
     * Returns true if and only if the sql statement is a commit command.
     * 
     * @return if is commit
     */
    private boolean isCommit() {
        return query.sql().equals(COMMIT);
    }

    /**
     * Returns true if and only if the sql statement is a rollback command.
     * 
     * @return if is rollback
     */
    private boolean isRollback() {
        return query.sql().equals(ROLLBACK);
    }

    /**
     * Commits the current transaction. Throws {@link RuntimeException} if
     * connection is in autoCommit mode.
     * 
     * @param subscriber
     * @param state
     * @param batchCounter
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void performCommit(Subscriber<? super T> subscriber, State state,
            AtomicInteger batchCounter) throws SQLException {
        int counter = batchCounter.get();
        if (counter > 0) {
            log.debug("executing batch size=" + counter);
            int count = sum(state.ps.executeBatch());
            log.debug("on commit batch executed returning count of ", count);
            // probably not needed
            batchCounter.set(0);
        }

        query.context().endTransactionObserve();
        if (subscriber.isUnsubscribed())
            return;

        log.debug("committing");
        Conditions.checkTrue(!Util.isAutoCommit(state.con));
        Util.commit(state.con);
        // must close before onNext so that connection is released and is
        // available to a query that might process the onNext
        close(state);

        if (subscriber.isUnsubscribed())
            return;

        // Non-zero indicates normal commit
        subscriber.onNext((T) Integer.valueOf(1));
        log.debug("committed");
        complete(subscriber);
    }

    /**
     * Rolls back the current transaction. Throws {@link RuntimeException} if
     * connection is in autoCommit mode.
     * 
     * @param subscriber
     * @param state
     */
    @SuppressWarnings("unchecked")
    private void performRollback(Subscriber<? super T> subscriber, State state) {
        log.debug("rolling back");
        query.context().endTransactionObserve();
        Conditions.checkTrue(!Util.isAutoCommit(state.con));
        Util.rollback(state.con);
        // must close before onNext so that connection is released and is
        // available to a query that might process the onNext
        close(state);
        subscriber.onNext((T) Integer.valueOf(0));
        log.debug("rolled back");
        complete(subscriber);
    }

    /**
     * Executes the prepared statement.
     * 
     * @param subscriber
     * @param batchCounter
     * 
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void performUpdate(final Subscriber<? super T> subscriber, State state,
            AtomicInteger batchCounter) throws SQLException {
        if (subscriber.isUnsubscribed()) {
            return;
        }
        int keysOption;
        if (query.returnGeneratedKeys()) {
            keysOption = Statement.RETURN_GENERATED_KEYS;
        } else {
            keysOption = Statement.NO_GENERATED_KEYS;
        }
        // TODO for batching we need to reuse prepared statements!
        state.ps = state.con.prepareStatement(query.sql(), keysOption);
        Util.setParameters(state.ps, parameters, query.names());

        if (subscriber.isUnsubscribed())
            return;

        final int count;
        try {
            if (query.batchSize() == 1) {
                log.debug("executing sql={}, parameters {}", query.sql(), parameters);
                count = state.ps.executeUpdate();
            } else if (batchCounter.incrementAndGet() == query.batchSize()) {
                log.debug("executing batch sql={}, parameters {}", query.sql(), parameters);
                count = sum(state.ps.executeBatch());
                // reset batch counter
                batchCounter.set(0);
            } else {
                log.debug("adding batch sql={}, parameters {}", query.sql(), parameters);
                state.ps.addBatch();
                count = 0;
            }
            log.debug("executed ps={}", state.ps);
            if (query.returnGeneratedKeys()) {
                log.debug("getting generated keys");
                ResultSet rs = state.ps.getGeneratedKeys();
                log.debug("returned generated key result set {}", rs);
                state.rs = rs;
                Observable<Parameter> params = Observable.just(new Parameter(state));
                Observable<Object> depends = Observable.empty();
                Observable<T> o = new QuerySelect(QuerySelect.RETURN_GENERATED_KEYS, params,
                        depends, query.context(), query.context().resultSetTransform())
                                .execute(query.returnGeneratedKeysFunction());
                Subscriber<T> sub = createSubscriber(subscriber);
                o.unsafeSubscribe(sub);
            }
        } catch (SQLException e) {
            throw new SQLException("failed to execute sql=" + query.sql(), e);
        }
        if (!query.returnGeneratedKeys()) {
            // must close before onNext so that connection is released and is
            // available to a query that might process the onNext
            close(state);
            if (subscriber.isUnsubscribed())
                return;
            log.debug("onNext");
            subscriber.onNext((T) (Integer) count);
            complete(subscriber);
        }
    }

    private static int sum(int[] values) {
        int sum = 0;
        for (int n : values) {
            sum += n;
        }
        return sum;
    }

    private Subscriber<T> createSubscriber(final Subscriber<? super T> subscriber) {
        return new Subscriber<T>(subscriber) {

            @Override
            public void onCompleted() {
                complete(subscriber);
            }

            @Override
            public void onError(Throwable e) {
                subscriber.onError(e);
            }

            @Override
            public void onNext(T t) {
                subscriber.onNext(t);
            }
        };
    }

    /**
     * Notify observer that sequence is complete.
     * 
     * @param subscriber
     * @param state
     */
    private void complete(Subscriber<? super T> subscriber) {
        if (!subscriber.isUnsubscribed()) {
            log.debug("onCompleted");
            subscriber.onCompleted();
        } else
            log.debug("unsubscribed");
    }

    /**
     * Notify observer of an error.
     * 
     * @param e
     * @param subscriber
     */
    private void handleException(Throwable e, Subscriber<? super T> subscriber) {
        log.debug("onError: ", e.getMessage());
        Exceptions.throwOrReport(e, subscriber);
    }

    /**
     * Cancels a running PreparedStatement, closing it and the current
     * Connection but only if auto commit mode.
     */
    private void close(State state) {
        // ensure close happens once only to avoid race conditions
        if (state.closed.compareAndSet(false, true)) {
            if (query.batchSize() == 1)
                Util.closeQuietly(state.ps);
            if (isCommit() || isRollback())
                Util.closeQuietly(state.con);
            else
                Util.closeQuietlyIfAutoCommit(state.con);
        }
    }

}
