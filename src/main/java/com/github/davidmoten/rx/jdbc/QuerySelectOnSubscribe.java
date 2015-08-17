package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

/**
 * OnSubscribe create method for a select query.
 */
final class QuerySelectOnSubscribe<T> implements OnSubscribe<T> {

    private static final Logger log = LoggerFactory.getLogger(QuerySelectOnSubscribe.class);

    /**
     * Returns an Observable of the results of pushing one set of parameters
     * through a select query.
     * 
     * @param params
     *            one set of parameters to be run with the query
     * @return
     */
    static <T> Observable<T> execute(QuerySelect query, List<Parameter> parameters,
            ResultSetMapper<? extends T> function) {
        return Observable.create(new QuerySelectOnSubscribe<T>(query, parameters, function));
    }

    private final ResultSetMapper<? extends T> function;
    private final QuerySelect query;
    private final List<Parameter> parameters;
    private final boolean stateProvided;

    /**
     * Constructor.
     * 
     * @param query
     * @param parameters
     */
    private QuerySelectOnSubscribe(QuerySelect query, List<Parameter> parameters,
            ResultSetMapper<? extends T> function) {
        this.query = query;
        this.parameters = parameters;
        this.function = function;
        this.stateProvided = query.sql().equals(QuerySelect.RETURN_GENERATED_KEYS);
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {
        State state = null;
        try {
            if (stateProvided) {
                state = (State) parameters.get(0).value();
                setupUnsubscription(subscriber, state);
            } else {
                state = new State();
                connectAndPrepareStatement(subscriber, state);
                setupUnsubscription(subscriber, state);
                executeQuery(subscriber, state);
            }
            subscriber.setProducer(new QuerySelectProducer<T>(function, subscriber, state.con,
                    state.ps, state.rs));
        } catch (Exception e) {
            query.context().endTransactionObserve();
            query.context().endTransactionSubscribe();
            try {
                if (state != null)
                    closeQuietly(state);
            } finally {
                handleException(e, subscriber);
            }
        }
    }

    private static <T> void setupUnsubscription(Subscriber<T> subscriber, final State state) {
        subscriber.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {
                closeQuietly(state);
            }
        }));
    }

    /**
     * Obtains connection, creates prepared statement and assigns parameters to
     * the prepared statement.
     * 
     * @param subscriber
     * @param state
     * 
     * @throws SQLException
     */
    private void connectAndPrepareStatement(Subscriber<? super T> subscriber, State state)
            throws SQLException {
        log.debug("connectionProvider={}", query.context().connectionProvider());
        if (!subscriber.isUnsubscribed()) {
            log.debug("getting connection");
            state.con = query.context().connectionProvider().get();
            log.debug("preparing statement,sql={}", query.sql());
            state.ps = state.con.prepareStatement(query.sql(), ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            log.debug("setting parameters");
            Util.setParameters(state.ps, parameters, query.names());
        }
    }

    /**
     * Executes the prepared statement.
     * 
     * @param subscriber
     * @param state
     * 
     * @throws SQLException
     */
    private void executeQuery(Subscriber<? super T> subscriber, State state) throws SQLException {
        if (!subscriber.isUnsubscribed()) {
            try {
                log.debug("executing ps");
                state.rs = query.resultSetTransform()
                        .call(query.context().resultSetTransform().call(state.ps.executeQuery()));
                log.debug("executed ps={}", state.ps);
            } catch (SQLException e) {
                throw new SQLException("failed to run sql=" + query.sql(), e);
            }
        }
    }

    /**
     * Tells observer about exception.
     * 
     * @param e
     * @param subscriber
     */
    private void handleException(Exception e, Subscriber<? super T> subscriber) {
        log.debug("onError: " + e.getMessage());
        if (subscriber.isUnsubscribed())
            log.debug("unsubscribed");
        else {
            subscriber.onError(e);
        }
    }

    /**
     * Closes connection resources (connection, prepared statement and result
     * set).
     * 
     * @param state
     */
    private static void closeQuietly(State state) {
        // ensure only closed once and avoid race conditions
        if (state.closed.compareAndSet(false, true)) {
            // set the state fields to null after closing for garbage
            // collection purposes
            log.debug("closing rs");
            Util.closeQuietly(state.rs);
            log.debug("closing ps");
            Util.closeQuietly(state.ps);
            log.debug("closing con");
            Util.closeQuietlyIfAutoCommit(state.con);
            log.debug("closed");
        }
    }

}
