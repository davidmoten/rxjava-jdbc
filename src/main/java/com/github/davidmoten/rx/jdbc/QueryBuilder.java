package com.github.davidmoten.rx.jdbc;

import rx.Observable;

/**
 * Builds base information for a query (either select or update).
 */
final public class QueryBuilder {

    /**
     * JDBC sql either select/update/insert.
     */
    private final String sql;

    /**
     * Parameters for the query corresponding to ? characters in the sql.
     */
    private Observable<Parameter> parameters = Observable.empty();

    /**
     * Observables to complete before the query is executed.
     */
    private Observable<?> depends = Observable.empty();

    /**
     * {@link Database} to use the query against.
     */
    private final Database db;

    /**
     * Execution context to use to run the query.
     */
    private final QueryContext context;

    /**
     * Constructor.
     * 
     * @param sql
     * @param db
     */
    public QueryBuilder(String sql, Database db) {
        this.sql = sql;
        this.db = db;
        this.context = db.queryContext();
    }

    /**
     * Appends the given parameters to the parameter list for the query. If
     * there are more parameters than required for one execution of the query
     * then more than one execution of the query will occur.
     * 
     * @param params
     */
    public <T> void parameters(Observable<T> params) {
        this.parameters = Observable.concat(parameters, params.map(Parameter.TO_PARAMETER));
    }

    /**
     * Appends the given parameter values to the parameter list for the query.
     * If there are more parameters than required for one execution of the query
     * then more than one execution of the query will occur.
     * 
     * @param objects
     */
    public void parameters(Object... objects) {
        for (Object object : objects)
            parameter(object);
    }

    /**
     * Appends a parameter to the parameter list for the query. If there are
     * more parameters than required for one execution of the query then more
     * than one execution of the query will occur.
     * 
     * @param value
     */
    public void parameter(Object value) {
        // TODO check on supported types?
        if (value instanceof Observable)
            throw new RuntimeException("use parameters() method not the parameter() method for an Observable");
        parameters(Observable.from(value));
    }

    /**
     * Appends a dependency to the dependencies that have to complete their
     * emitting before the query is executed.
     * 
     * @param dependency
     */
    public void dependsOn(Observable<?> dependency) {
        depends = Observable.concat(depends, dependency);
    }

    /**
     * Appends a dependency on the result of the last transaction (
     * <code>true</code> for commit or <code>false</code> for rollback) to the
     * dependencies that have to complete their emitting before the query is
     * executed.
     */
    public void dependsOnLastTransaction() {
        dependsOn(db.lastTransactionResult());
    }

    /**
     * Returns the sql of the query.
     * 
     * @return sql
     */
    public String sql() {
        return sql;
    }

    /**
     * Returns the parameters for the query.
     * 
     * @return parameters
     */
    public Observable<Parameter> parameters() {
        return parameters;
    }

    /**
     * Returns the dependencies of the query.
     * 
     * @return dependencies
     */
    public Observable<?> depends() {
        return depends;
    }

    /**
     * Returns the query's {@link QueryContext}.
     * 
     * @return context
     */
    public QueryContext context() {
        return context;
    }
}
