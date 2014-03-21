package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

final class QuerySelectOperation {

    private static final Logger log = LoggerFactory.getLogger(QuerySelectOperation.class);

    /**
     * Returns an Observable of the results of pushing one set of parameters
     * through a select query.
     * 
     * @param connectionResource
     * 
     * @param params
     *            one set of parameters to be run with the query
     * @return
     */
    static Observable<ResultSet> execute(QuerySelect query, ConnectionResource connectionResource,
            List<Parameter> parameters) {
        return Observable.create(new QuerySelectOnSubscribe(connectionResource, query, parameters));
    }

    /**
     * OnSubscribe create method for a select query.
     */
    private static class QuerySelectOnSubscribe implements OnSubscribe<ResultSet> {

        private boolean keepGoing = true;
        private final List<Parameter> parameters;
        private final QuerySelect query;
        private final ConnectionResource connectionResource;

        /**
         * Constructor.
         * 
         * @param connectionResource
         * 
         * @param query
         * @param parameters
         */
        private QuerySelectOnSubscribe(ConnectionResource connectionResource, QuerySelect query,
                List<Parameter> parameters) {
            this.connectionResource = connectionResource;
            this.query = query;
            this.parameters = parameters;
        }

        @Override
        public void call(Subscriber<? super ResultSet> subscriber) {
            try {
                connectAndPrepareStatement(subscriber);
                executeQuery(subscriber);
                while (keepGoing) {
                    processRow(subscriber);
                }
                complete(subscriber);
            } catch (Exception e) {
                handleException(e, subscriber);
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
        private void connectAndPrepareStatement(Subscriber<? super ResultSet> subscriber) throws SQLException {
            log.debug("connectionProvider=" + query.context().connectionProvider());
            checkSubscription(subscriber);
            if (keepGoing) {
                log.debug("preparing statement,sql=" + query.sql());
                connectionResource.prepareStatement(query.sql());
                log.debug("setting parameters");

                connectionResource.setParameters(parameters);
            }
        }

        /**
         * Executes the prepared statement.
         * 
         * @param subscriber
         * 
         * @throws SQLException
         */
        private void executeQuery(Subscriber<? super ResultSet> subscriber) throws SQLException {
            checkSubscription(subscriber);
            if (keepGoing) {
                try {
                    connectionResource.executeQuery();
                } catch (SQLException e) {
                    throw new SQLException("failed to run sql=" + query.sql(), e);
                }
            }
        }

        /**
         * Processes each row of the {@link ResultSet}.
         * 
         * @param subscriber
         * 
         * @throws SQLException
         */
        private void processRow(Subscriber<? super ResultSet> subscriber) throws SQLException {
            checkSubscription(subscriber);
            if (!keepGoing)
                return;
            if (connectionResource.next()) {
                log.trace("onNext");
                subscriber.onNext(connectionResource.rs());
            } else
                keepGoing = false;
        }

        /**
         * Tells observer that stream is complete and closes resources.
         * 
         * @param subscriber
         */
        private void complete(Subscriber<? super ResultSet> subscriber) {
            if (subscriber.isUnsubscribed()) {
                log.debug("unsubscribed");
            } else {
                log.debug("onCompleted");
                subscriber.onCompleted();
            }
        }

        /**
         * Tells observer about exception.
         * 
         * @param e
         * @param subscriber
         */
        private void handleException(Exception e, Subscriber<? super ResultSet> subscriber) {
            log.debug("onError: " + e.getMessage());
            if (subscriber.isUnsubscribed())
                log.debug("unsubscribed");
            else {
                subscriber.onError(e);
            }
        }

        /**
         * If subscribe unsubscribed sets keepGoing to false.
         * 
         * @param subscriber
         */
        private void checkSubscription(Subscriber<? super ResultSet> subscriber) {
            if (subscriber.isUnsubscribed()) {
                keepGoing = false;
                log.debug("unsubscribing");
            }
        }

    }
}
