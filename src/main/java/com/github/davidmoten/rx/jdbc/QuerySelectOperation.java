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
import rx.functions.Func1;

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
            Func1<? super ResultSet, T> function) {
        return Observable.create(new QuerySelectOnSubscribe<T>(query, parameters, function));
    }

    /**
     * OnSubscribe create method for a select query.
     */
    private static class QuerySelectOnSubscribe<T> implements OnSubscribe<T> {

        private boolean keepGoing = true;
        private final List<Parameter> parameters;
        private final QuerySelect query;
        private Connection con;
        private PreparedStatement ps;
        private ResultSet rs;
        private final Func1<? super ResultSet, T> function;

        /**
         * Constructor.
         * 
         * @param query
         * @param parameters
         */
        private QuerySelectOnSubscribe(QuerySelect query, List<Parameter> parameters,
                Func1<? super ResultSet, T> function) {
            this.query = query;
            this.parameters = parameters;
            this.function = function;
        }

        @Override
        public void call(Subscriber<? super T> subscriber) {
            try {
                connectAndPrepareStatement(subscriber);
                executeQuery(subscriber);
                subscriber
                        .setProducer(new QuerySelectProducer<T>(function, subscriber, con, ps, rs));
            } catch (Exception e) {
                try {
                    closeQuietly();
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
         * 
         * @throws SQLException
         */
        private void connectAndPrepareStatement(Subscriber<? super T> subscriber)
                throws SQLException {
            log.debug("connectionProvider={}", query.context().connectionProvider());
            checkSubscription(subscriber);
            if (keepGoing) {
                log.debug("getting connection");
                con = query.context().connectionProvider().get();
                log.debug("preparing statement,sql={}", query.sql());
                ps = con.prepareStatement(query.sql());
                log.debug("setting parameters");
                Util.setParameters(ps, parameters);
            }
        }

        /**
         * Executes the prepared statement.
         * 
         * @param subscriber
         * 
         * @throws SQLException
         */
        private void executeQuery(Subscriber<? super T> subscriber) throws SQLException {
            checkSubscription(subscriber);
            if (keepGoing) {
                try {
                    log.debug("executing ps");
                    rs = ps.executeQuery();
                    log.debug("executed ps={}", ps);
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
         */
        private void closeQuietly() {
            log.debug("closing rs");
            Util.closeQuietly(rs);
            log.debug("closing ps");
            Util.closeQuietly(ps);
            log.debug("closing con");
            Util.closeQuietlyIfAutoCommit(con);
            log.debug("closed");
        }

        /**
         * If subscribe unsubscribed sets keepGoing to false.
         * 
         * @param subscriber
         */
        private void checkSubscription(Subscriber<? super T> subscriber) {
            if (subscriber.isUnsubscribed()) {
                keepGoing = false;
                log.debug("unsubscribing");
            }
        }

    }
}
