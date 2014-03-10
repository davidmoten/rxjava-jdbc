package com.github.davidmoten.rx.jdbc;

import rx.Scheduler;

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

    public void beginTransactionObserve() {
        db.beginTransactionObserve();

    }

    public void beginTransactionSubscribe() {
        db.beginTransactionSubscribe();
    }

    public void endTransactionSubscribe() {
        db.endTransactionSubscribe();
    }

    public void endTransactionObserve() {
        db.endTransactionObserve();
    }

}