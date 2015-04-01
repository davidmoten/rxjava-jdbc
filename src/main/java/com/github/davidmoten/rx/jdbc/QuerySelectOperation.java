package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

final class QuerySelectOperation {

    private static final Logger log = LoggerFactory.getLogger(QuerySelectOperation.class);

    /**
     * Returns an Observable of the results of pushing one set of parameters
     * through a select query.
     * 
     * @param params
     *            one set of parameters to be run with the query
     * @return
     */
    static <T> Observable<T> execute(QuerySelect query, List<Parameter> parameters,
            Func1<? super ResultSet, ? extends T> function) {
        return Observable.create(new QuerySelectOnSubscribe<T>(query, parameters, function));
    }

    /**
     * OnSubscribe create method for a select query.
     */
    private static class QuerySelectOnSubscribe<T> implements OnSubscribe<T> {

        private final Func1<? super ResultSet, ? extends T> function;
        private final QuerySelect query;
        private final List<Parameter> parameters;

        private static class State {
            boolean keepGoing = true;
            Connection con;
            PreparedStatement ps;
            ResultSet rs;
        }

        /**
         * Constructor.
         * 
         * @param query
         * @param parameters
         */
        private QuerySelectOnSubscribe(QuerySelect query, List<Parameter> parameters,
                Func1<? super ResultSet, ? extends T> function) {
            this.query = query;
            this.parameters = parameters;
            this.function = function;
        }

        @Override
        public void call(Subscriber<? super T> subscriber) {
            final State state = new State();
            try {
                connectAndPrepareStatement(subscriber,state);
                executeQuery(subscriber,state);
                subscriber
                        .setProducer(new QuerySelectProducer<T>(function, subscriber, state.con, state.ps, state.rs));
                // this is required for the case when
                // "select count(*) from tbl".take(1) is called which enables
                // the backpressure path and 1 is requested and end of result
                // set is is not detected so onComplete action of closing does
                // not happen.
                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        closeQuietly(state);
                    }
                }));
            } catch (Exception e) {
                try {
                    closeQuietly(state);
                } finally {
                    handleException(e, subscriber);
                }
            }
        }

        /**
         * Obtains connection, creates prepared statement and assigns parameters
         * to the prepared statement.
         * 
         * @param subscriber
         * @param state 
         * 
         * @throws SQLException
         */
        private void connectAndPrepareStatement(Subscriber<? super T> subscriber, State state)
                throws SQLException {
            log.debug("connectionProvider={}", query.context().connectionProvider());
            checkSubscription(subscriber,state);
            if (state.keepGoing) {
                log.debug("getting connection");
                state.con = query.context().connectionProvider().get();
                log.debug("preparing statement,sql={}", query.sql());
                state.ps = state.con.prepareStatement(query.sql());
                log.debug("setting parameters");
                Util.setParameters(state.ps, parameters);
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
            checkSubscription(subscriber,state);
            if (state.keepGoing) {
                try {
                    log.debug("executing ps");
                    state.rs = state.ps.executeQuery();
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
         * Closes connection resources (connection, prepared statement and
         * result set).
         * @param state 
         */
        private void closeQuietly(State state) {
            log.debug("closing rs");
            Util.closeQuietly(state.rs);
            log.debug("closing ps");
            Util.closeQuietly(state.ps);
            log.debug("closing con");
            Util.closeQuietlyIfAutoCommit(state.con);
            log.debug("closed");
        }

        /**
         * If subscribe unsubscribed sets keepGoing to false.
         * 
         * @param subscriber
         */
        private void checkSubscription(Subscriber<? super T> subscriber,State state) {
            if (subscriber.isUnsubscribed()) {
                state.keepGoing = false;
                log.debug("unsubscribing");
            }
        }

    }
}
