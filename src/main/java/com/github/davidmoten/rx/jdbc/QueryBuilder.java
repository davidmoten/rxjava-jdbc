package com.github.davidmoten.rx.jdbc;

import com.github.davidmoten.guavamini.Preconditions;

import rx.Observable;

/**
 * Builds base information for a query (either select or update).
 */
final class QueryBuilder {

    /**
     * JDBC sql either select/update/insert.
     */
    private String sql;

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
    QueryBuilder(String sql, Database db) {
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
    <T> void parameters(Observable<T> params) {
        this.parameters = Observable.concat(parameters, params.map(Parameter.TO_PARAMETER));
    }

    /**
     * Appends the given parameter values to the parameter list for the query.
     * If there are more parameters than required for one execution of the query
     * then more than one execution of the query will occur.
     * 
     * @param objects
     */
    void parameters(Object... objects) {
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
    void parameter(Object value) {
        // TODO check on supported types?
        if (value instanceof Observable)
            throw new IllegalArgumentException(
                    "use parameters() method not the parameter() method for an Observable");
        parameters(Observable.just(value));
    }

    /**
     * Sets a named parameter. If name is null throws a
     * {@link NullPointerException}. If value is instance of Observable then
     * throws an {@link IllegalArgumentException}.
     * 
     * @param name
     *            the parameter name. Cannot be null.
     * @param value
     *            the parameter value
     */
    void parameter(String name, Object value) {
        Preconditions.checkNotNull(name, "parameter name cannot be null");
        if (value instanceof Observable)
            throw new IllegalArgumentException(
                    "use parameters() method not the parameter() method for an Observable");
        this.parameters = parameters.concatWith(Observable.just(new Parameter(name, value)));
    }

    /**
     * Appends a dependency to the dependencies that have to complete their
     * emitting before the query is executed.
     * 
     * @param dependency
     */
    void dependsOn(Observable<?> dependency) {
        depends = Observable.concat(depends, dependency);
    }

    /**
     * Appends a dependency on the result of the last transaction (
     * <code>true</code> for commit or <code>false</code> for rollback) to the
     * dependencies that have to complete their emitting before the query is
     * executed.
     */
    void dependsOnLastTransaction() {
        dependsOn(db.lastTransactionResult());
    }

    /**
     * Returns the sql of the query.
     * 
     * @return sql
     */
    String sql() {
        return sql;
    }

    /**
     * Returns the parameters for the query.
     * 
     * @return parameters
     */
    Observable<Parameter> parameters() {
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
    QueryContext context() {
        return context;
    }

    void clearParameters() {
        this.parameters = Observable.empty();
    }

    void setSql(String sql) {
        this.sql = sql;
    }
}
