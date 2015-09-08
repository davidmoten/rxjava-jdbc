package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Scheduler;
import rx.functions.Func1;

/**
 * The threading and database connection context for mutliple jdbc queries.
 * 
 */
final class QueryContext {

    private final Database db;

    /**
     * Constructor.
     * 
     * @param executor
     * @param connectionProvider
     */
    QueryContext(Database db) {
        this.db = db;
    }

    /**
     * Returns the scheduler service to use to run queries with this context.
     * 
     * @return
     */
    Scheduler scheduler() {
        return db.currentScheduler();
    }

    /**
     * Returns the connection provider for queries with this context.
     * 
     * @return
     */
    ConnectionProvider connectionProvider() {
        return db.connectionProvider();
    }

    void beginTransactionObserve() {
        db.beginTransactionObserve();

    }

    void beginTransactionSubscribe() {
        db.beginTransactionSubscribe();
    }

    void endTransactionSubscribe() {
        db.endTransactionSubscribe();
    }

    void endTransactionObserve() {
        db.endTransactionObserve();
    }

    Func1<ResultSet, ? extends ResultSet> resultSetTransform() {
        return db.getResultSetTransform();
    }
}