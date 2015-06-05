package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Conditions.checkNotNull;
import static com.github.davidmoten.rx.jdbc.Queries.bufferedParameters;

import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.functions.Func1;

/**
 * Always emits an Observable<Integer> of size 1 containing the number of
 * affected records.
 */
final public class QueryUpdate implements Query {

    private final String sql;
    private final Observable<Parameter> parameters;
    private final QueryContext context;
    private final Observable<?> depends;

    /**
     * Private constructor.
     * 
     * @param sql
     * @param parameters
     * @param depends
     * @param context
     */
    private QueryUpdate(String sql, Observable<Parameter> parameters, Observable<?> depends,
            QueryContext context) {
        checkNotNull(sql);
        checkNotNull(parameters);
        checkNotNull(depends);
        checkNotNull(context);
        this.sql = sql;
        this.parameters = parameters;
        this.depends = depends;
        this.context = context;
    }

    @Override
    public String sql() {
        return sql;
    }

    @Override
    public Observable<Parameter> parameters() {
        return parameters;
    }

    @Override
    public QueryContext context() {
        return context;
    }

    @Override
    public String toString() {
        return "QueryUpdate [sql=" + sql + "]";
    }

    @Override
    public Observable<?> depends() {
        return depends;
    }

    /**
     * Returns the results of an update query. Should be an {@link Observable}
     * of size 1 containing the number of records affected by the update (or
     * insert) statement.
     * 
     * @param query
     * @return
     */
    public Observable<Integer> count() {
        return bufferedParameters(this)
        // execute query for each set of parameters
                .concatMap(executeOnce());
    }

    /**
     * Returns a {@link Func1} that itself returns the results of pushing
     * parameters through an update query.
     * 
     * @param query
     * @return
     */
    private Func1<List<Parameter>, Observable<Integer>> executeOnce() {
        return new Func1<List<Parameter>, Observable<Integer>>() {
            @Override
            public Observable<Integer> call(final List<Parameter> params) {
                if (sql.equals(QueryUpdateOnSubscribe.BEGIN_TRANSACTION)) {
                    context.beginTransactionSubscribe();
                }
                Observable<Integer> result = executeOnce(params).subscribeOn(context.scheduler());
                if (sql.equals(QueryUpdateOnSubscribe.COMMIT)
                        || sql.equals(QueryUpdateOnSubscribe.ROLLBACK))
                    context.endTransactionSubscribe();
                return result;
            }
        };
    }

    /**
     * Returns the results of an update query. Should return an
     * {@link Observable} of size one containing the rows affected count.
     * 
     * @param query
     * @param parameters
     * @return
     */
    private Observable<Integer> executeOnce(final List<Parameter> parameters) {
        return QueryUpdateOnSubscribe.execute(this, parameters);
    }

    /**
     * Builds a {@link QueryUpdate}.
     */
    final public static class Builder {

        /**
         * Standard query builder.
         */
        private final QueryBuilder builder;

        /**
         * Constructor.
         * 
         * @param sql
         * @param db
         */
        public Builder(String sql, Database db) {
            this.builder = new QueryBuilder(sql, db);
        }

        /**
         * Appends the given parameters to the parameter list for the query. If
         * there are more parameters than required for one execution of the
         * query then more than one execution of the query will occur.
         * 
         * @param parameters
         * @return this
         */
        public <T> Builder parameters(Observable<T> parameters) {
            builder.parameters(parameters);
            return this;
        }

        /**
         * Appends the given parameter values to the parameter list for the
         * query. If there are more parameters than required for one execution
         * of the query then more than one execution of the query will occur.
         * 
         * @param objects
         * @return this
         */
        public Builder parameters(Object... objects) {
            builder.parameters(objects);
            return this;
        }

        /**
         * Appends a parameter to the parameter list for the query. If there are
         * more parameters than required for one execution of the query then
         * more than one execution of the query will occur.
         * 
         * @param value
         * @return this
         */
        public Builder parameter(Object value) {
            builder.parameter(value);
            return this;
        }

        /**
         * Appends a parameter to the parameter list for the query for a CLOB
         * parameter and handles null appropriately. If there are more
         * parameters than required for one execution of the query then more
         * than one execution of the query will occur.
         * 
         * @param value
         *            the string to insert in the CLOB column
         * @return this
         */
        public Builder parameterClob(String value) {
            builder.parameter(Database.toSentinelIfNull(value));
            return this;
        }

        /**
         * Appends a parameter to the parameter list for the query for a CLOB
         * parameter and handles null appropriately. If there are more
         * parameters than required for one execution of the query then more
         * than one execution of the query will occur.
         * 
         * @param value
         * @return this
         */
        public Builder parameterBlob(byte[] bytes) {
            builder.parameter(Database.toSentinelIfNull(bytes));
            return this;
        }

        /**
         * Appends a dependency to the dependencies that have to complete their
         * emitting before the query is executed.
         * 
         * @param dependency
         * @return this
         */
        public Builder dependsOn(Observable<?> dependency) {
            builder.dependsOn(dependency);
            return this;
        }

        /**
         * Appends a dependency on the result of the last transaction (
         * <code>true</code> for commit or <code>false</code> for rollback) to
         * the dependencies that have to complete their emitting before the
         * query is executed.
         * 
         * @return this
         */
        public Builder dependsOnLastTransaction() {
            builder.dependsOnLastTransaction();
            return this;
        }

        public ReturnGeneratedKeysBuilder<Object> returnGeneratedKeys() {
            return new ReturnGeneratedKeysBuilder<Object>(builder);
        }

        /**
         * Returns an {@link Observable} with the count of rows affected by the
         * update statement.
         * 
         * @return
         */
        public Observable<Integer> count() {
            return new QueryUpdate(builder.sql(), builder.parameters(), builder.depends(),
                    builder.context()).count();
        }

        /**
         * Returns an {@link Operator} to allow the query to be pushed
         * parameters via the {@link Observable#lift(Operator)} method.
         * 
         * @return operator that acts on parameters
         */
        public Operator<Integer, Object> parameterOperator() {
            return new QueryUpdateOperator<Object>(this, OperatorType.PARAMETER);
        }

        /**
         * Returns an {@link Operator} to allow the query to be pushed
         * dependencies via the {@link Observable#lift(Operator)} method.
         * 
         * @return operator that acts on dependencies
         */
        public Operator<Integer, Object> dependsOnOperator() {
            return new QueryUpdateOperator<Object>(this, OperatorType.DEPENDENCY);
        }

        /**
         * Returns an {@link Operator} to allow the query to be run once per
         * parameter list in the source.
         * 
         * @return
         */
        public Operator<Observable<Integer>, Observable<Object>> parameterListOperator() {
            return new QueryUpdateOperatorFromObservable<Object>(this);
        }

        public Builder clearParameters() {
            builder.clearParameters();
            return this;
        }
    }

    static class ReturnGeneratedKeysBuilder<T> {

        private final QueryBuilder builder;

        public ReturnGeneratedKeysBuilder(QueryBuilder builder) {
            this.builder = builder;
        }

        // /**
        // * Transforms the results using the given function.
        // *
        // * @param function
        // * @return
        // */
        // public <T> Observable<T> get(ResultSetMapper<? extends T> function) {
        // return new QuerySelect(builder.sql(), builder.parameters(),
        // builder.depends(),
        // builder.context()).execute(function);
        // }
    }
}
