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
    private final int batchSize;
    private final int fetchSize;

    QueryContext(Database db) {
        this(db, 1, 0);
    }

    public QueryContext(Database db, int batchSize, int fetchSize) {
        this.db = db;
        this.batchSize = batchSize;
        this.fetchSize = fetchSize;
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

    void setupBatching() {
        db.batching(batchSize);
    }

    boolean isTransactionOpen() {
        return db.isTransactionOpen();
    }

    Func1<ResultSet, ? extends ResultSet> resultSetTransform() {
        return db.getResultSetTransform();
    }

    QueryContext batched(int batchSize) {
        return new QueryContext(db, batchSize, fetchSize);
    }

    int batchSize() {
        return batchSize;
    }

    QueryContext fetchSize(int fetchSize) {
        return new QueryContext(db, batchSize, fetchSize);
    }

    int fetchSize() {
        return fetchSize;
    }

}