package com.github.davidmoten.rx.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.Exceptions;
import rx.functions.Action0;
import rx.functions.Action1;
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
     * @param state
     * @param doOnBatchCommit
     * @return
     */
    static <T> Observable<T> execute(QueryUpdate<T> query, final State state, List<Parameter> parameters, final Action1<int[]> doOnBatchCommit) {
        return Observable.create(new QueryUpdateOnSubscribe<T>(query, state, parameters, doOnBatchCommit));
    }

    /**
     * The query to be executed.
     */
    private final QueryUpdate<T> query;

    private final State state;

    /**
     * The parameters to run the query against (may be a subset of the query
     * parameters specified in the query because the query may be run multiple
     * times with multiple sets of parameters).
     */
    private final List<Parameter> parameters;

    private final Action1<int[]> doOnBatchCommit;

    /**
     * Constructor.
     * @param query
     * @param state
     * @param parameters
     * @param doOnBatchCommit
     */
    private QueryUpdateOnSubscribe(QueryUpdate<T> query, State state, List<Parameter> parameters, Action1<int[]> doOnBatchCommit) {
        this.query = query;
        this.state = state;
        this.parameters = parameters;
        this.doOnBatchCommit = doOnBatchCommit;
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {

        try {
            if (isBeginTransaction())
                performBeginTransaction(subscriber);
            else {
                getConnection(state);
                subscriber.add(createUnsubscriptionAction(state));
                if (isCommit())
                    performCommit(subscriber, state);
                else if (isRollback())
                    performRollback(subscriber, state);
                else
                    performUpdate(subscriber, state);
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
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void performCommit(Subscriber<? super T> subscriber, State state) throws SQLException {
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
     *
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void performUpdate(final Subscriber<? super T> subscriber, State state) throws SQLException {
        if (subscriber.isUnsubscribed()) {
            return;
        }

        state.ps = getPreparedStatement(state);
        Util.setParameters(state.ps, parameters, query.names());

        if (subscriber.isUnsubscribed())
            return;

        final Batch batch = Batch.get();

        int count = -1;
        try {
            log.debug("executing sql={}, parameters {}", query.sql(), parameters);

            if (batch.size == 1) {
                count = state.ps.executeUpdate();
            } else {
                state.ps.addBatch();
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
        if (batch.size > 1 && batch.complete()) {
            int[] affectedRowCounts = state.ps.executeBatch();
            if (doOnBatchCommit != null) {
                doOnBatchCommit.call(affectedRowCounts);
            }
            count = sum(affectedRowCounts);
        }

        if (!query.returnGeneratedKeys()) {
            // must close before onNext so that connection is released and is
            // available to a query that might process the onNext
            close(state);
            if (subscriber.isUnsubscribed())
                return;

            if (count != -1) {
                log.debug("onNext");
                subscriber.onNext((T) (Integer) count);
            }
            complete(subscriber);
        }
    }

    public PreparedStatement getPreparedStatement(State state) throws SQLException {
        int keysOption;
        if (query.returnGeneratedKeys()) {
            keysOption = Statement.RETURN_GENERATED_KEYS;
        } else {
            keysOption = Statement.NO_GENERATED_KEYS;
        }
        if (Batch.get().getPreparedStatement() == null
                || !Batch.get().getPreparedStatement().getSql().equals(query.sql())
                || Batch.get().getPreparedStatement().getKeysOption() != keysOption) {
            /*Batch.get().setPreparedStatement(
                    new PreparedStatementBatch(state.con.prepareStatement(query.sql(), keysOption), query.sql(), keysOption));*/
            state.con.prepareStatement(query.sql(), keysOption);
        }
        return Batch.get().getPreparedStatement();
    }

    static int sum(int[] values) {
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
            PreparedStatement ps = state.ps;
            if (!Batch.get().enabled()) {
                close(ps);
            }
            if (isCommit() || isRollback()) {
                close(ps);
                Util.closeQuietly(state.con);
            } else {
                Util.closeQuietlyIfAutoCommit(state.con);
            }
        }
    }

    private void close(PreparedStatement ps) {
        Util.closeQuietly(ps);
        Batch.get().setPreparedStatement(null);
    }

}
