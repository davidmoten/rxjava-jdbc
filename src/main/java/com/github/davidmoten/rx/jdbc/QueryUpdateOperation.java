package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

/**
 * Executes the update query.
 */
class QueryUpdateOperation {

    private static final Logger log = LoggerFactory.getLogger(QueryUpdateOperation.class);

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
    static Observable<Integer> execute(QueryUpdate query, List<Parameter> parameters) {
        return Observable.create(new QueryUpdateOnSubscribe(query, parameters));
    }

    private static class QueryUpdateOnSubscribe implements OnSubscribe<Integer> {

        private boolean keepGoing = true;

        /**
         * The query to be executed.
         */
        private final QueryUpdate query;

        /**
         * The parameters to run the query against (may be a subset of the query
         * parameters specified in the query because the query may be run
         * multiple times with multiple sets of parameters).
         */
        private final List<Parameter> parameters;

        /**
         * Query connection.
         */
        private Connection con;

        /**
         * Prepared statement for the query.
         */
        private PreparedStatement ps;

        /**
         * Constructor.
         * 
         * @param query
         * @param parameters
         */
        private QueryUpdateOnSubscribe(QueryUpdate query, List<Parameter> parameters) {
            this.query = query;
            this.parameters = parameters;
        }

        @Override
        public void call(Subscriber<? super Integer> subscriber) {
            try {

                if (isBeginTransaction())
                    performBeginTransaction(subscriber);
                else {
                    getConnection();
                    if (isCommit())
                        performCommit(subscriber);
                    else if (isRollback())
                        performRollback(subscriber);
                    else
                        performUpdate(subscriber);
                    close();
                }

                complete(subscriber);

            } catch (Exception e) {
                try {
                    close();
                } finally {
                    handleException(e, subscriber);
                }
            }
        }

        private boolean isBeginTransaction() {
            return query.sql().equals(BEGIN_TRANSACTION);
        }

        private void performBeginTransaction(Subscriber<? super Integer> subscriber) {
            query.context().beginTransactionObserve();
            log.debug("beginTransaction emitting 1");
            subscriber.onNext(Integer.valueOf(1));
            log.debug("emitted 1");
        }

        /**
         * Gets the current connection.
         */
        private void getConnection() {
            log.debug("getting connection");
            con = query.context().connectionProvider().get();
            log.debug("cp=" + query.context().connectionProvider());
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
         */
        private void performCommit(Subscriber<? super Integer> subscriber) {
            query.context().endTransactionObserve();
            checkSubscription(subscriber);
            if (!keepGoing)
                return;

            log.debug("committing");
            Conditions.checkTrue(!Util.isAutoCommit(con));
            Util.commit(con);
            // must close before onNext so that connection is released and is
            // available to a query that might process the onNext
            close();

            checkSubscription(subscriber);
            if (!keepGoing)
                return;

            subscriber.onNext(Integer.valueOf(1));
            log.debug("committed");
        }

        /**
         * Rolls back the current transaction. Throws {@link RuntimeException}
         * if connection is in autoCommit mode.
         * 
         * @param subscriber
         */
        private void performRollback(Subscriber<? super Integer> subscriber) {
            log.debug("rolling back");
            query.context().endTransactionObserve();
            Conditions.checkTrue(!Util.isAutoCommit(con));
            Util.rollback(con);
            // must close before onNext so that connection is released and is
            // available to a query that might process the onNext
            close();
            subscriber.onNext(Integer.valueOf(0));
            log.debug("rolled back");
        }

        /**
         * Executes the prepared statement.
         * 
         * @param subscriber
         * 
         * @throws SQLException
         */
        private void performUpdate(Subscriber<? super Integer> subscriber) throws SQLException {
            checkSubscription(subscriber);
            if (!keepGoing)
                return;

            ps = con.prepareStatement(query.sql());
            Util.setParameters(ps, parameters);

            checkSubscription(subscriber);
            if (!keepGoing)
                return;

            int count;
            try {
                log.debug("executing sql=" + query.sql() + ", parameters" + parameters);
                count = ps.executeUpdate();
                log.debug("executed ps=" + ps);
            } catch (SQLException e) {
                throw new SQLException("failed to execute sql=" + query.sql(), e);
            }
            // must close before onNext so that connection is released and is
            // available to a query that might process the onNext
            close();
            checkSubscription(subscriber);
            if (!keepGoing)
                return;
            log.debug("onNext");
            subscriber.onNext((count));
        }

        /**
         * Notify observer that sequence is complete.
         * 
         * @param subscriber
         */
        private void complete(Subscriber<? super Integer> subscriber) {
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
        private void handleException(Exception e, Subscriber<? super Integer> subscriber) {
            log.debug("onError: " + e.getMessage());
            if (subscriber.isUnsubscribed())
                log.debug("unsubscribed");
            else {
                subscriber.onError(e);
            }
        }

        /**
         * Cancels a running PreparedStatement, closing it and the current
         * Connection but only if auto commit mode.
         */
        private void close() {
            Util.closeQuietly(ps);
            if (isCommit() || isRollback())
                Util.closeQuietly(con);
            else
                Util.closeQuietlyIfAutoCommit(con);
        }

        private void checkSubscription(Subscriber<? super Integer> subscriber) {
            if (subscriber.isUnsubscribed()) {
                keepGoing = false;
                log.debug("unsubscribing");
            }
        }

    }

}
