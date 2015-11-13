package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Conditions.checkNotNull;
import static com.github.davidmoten.rx.jdbc.Queries.bufferedParameters;

import java.sql.ResultSet;
import java.util.List;

import com.github.davidmoten.rx.jdbc.NamedParameters.JdbcQuery;
import com.github.davidmoten.rx.jdbc.tuple.Tuple2;
import com.github.davidmoten.rx.jdbc.tuple.Tuple3;
import com.github.davidmoten.rx.jdbc.tuple.Tuple4;
import com.github.davidmoten.rx.jdbc.tuple.Tuple5;
import com.github.davidmoten.rx.jdbc.tuple.Tuple6;
import com.github.davidmoten.rx.jdbc.tuple.Tuple7;
import com.github.davidmoten.rx.jdbc.tuple.TupleN;
import com.github.davidmoten.rx.jdbc.tuple.Tuples;
import com.github.davidmoten.util.Preconditions;

import rx.Observable;
import rx.Observable.Operator;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Always emits an Observable<Integer> of size 1 containing the number of
 * affected records.
 * 
 * @param <T>
 *            type of returned observable (Integer for count, custom for
 *            returning generated keys)
 */
final public class QueryUpdate<T> implements Query {

    private final JdbcQuery jdbcQuery;
    private final Observable<Parameter> parameters;
    private final QueryContext context;
    private final Observable<?> depends;
    // nullable!
    private final ResultSetMapper<? extends T> returnGeneratedKeysFunction;

    /**
     * Private constructor.
     * 
     * @param sql
     * @param parameters
     * @param depends
     * @param context
     * @param returnGeneratedKeysFunction
     *            nullable!
     * @param batchSize
     */
    private QueryUpdate(String sql, Observable<Parameter> parameters, Observable<?> depends,
            QueryContext context, ResultSetMapper<? extends T> returnGeneratedKeysFunction,
            int batchSize) {
        checkNotNull(sql);
        checkNotNull(parameters);
        checkNotNull(depends);
        checkNotNull(context);
        this.jdbcQuery = NamedParameters.parse(sql);
        this.parameters = parameters;
        this.depends = depends;
        this.context = context;
        this.returnGeneratedKeysFunction = returnGeneratedKeysFunction;
    }

    @Override
    public String sql() {
        return jdbcQuery.sql();
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
        return "QueryUpdate [sql=" + sql() + "]";
    }

    @Override
    public Observable<?> depends() {
        return depends;
    }

    @Override
    public List<String> names() {
        return jdbcQuery.names();
    }

    /**
     * Returns the results of an update query. Should be an {@link Observable}
     * of size 1 containing the number of records affected by the update (or
     * insert) statement.
     * 
     * @param query
     * @return
     */
    @SuppressWarnings("unchecked")
    public Observable<Integer> count() {
        return (Observable<Integer>) QueryUpdate.get(this);
    }

    public ResultSetMapper<? extends T> returnGeneratedKeysFunction() {
        return returnGeneratedKeysFunction;
    }

    boolean returnGeneratedKeys() {
        return returnGeneratedKeysFunction != null;
    }

    static <T> Observable<T> get(QueryUpdate<T> queryUpdate) {
        return bufferedParameters(queryUpdate)
                // execute query for each set of parameters
                .concatMap(queryUpdate.executeOnce());
    }

    /**
     * Returns a {@link Func1} that itself returns the results of pushing
     * parameters through an update query.
     * 
     * @param query
     * @return
     */
    private Func1<List<Parameter>, Observable<T>> executeOnce() {
        return new Func1<List<Parameter>, Observable<T>>() {
            @Override
            public Observable<T> call(final List<Parameter> params) {
                if (jdbcQuery.sql().equals(QueryUpdateOnSubscribe.BEGIN_TRANSACTION)) {
                    context.beginTransactionSubscribe();
                }
                Observable<T> result = executeOnce(params).subscribeOn(context.scheduler());
                if (jdbcQuery.sql().equals(QueryUpdateOnSubscribe.COMMIT)
                        || jdbcQuery.sql().equals(QueryUpdateOnSubscribe.ROLLBACK))
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
    private Observable<T> executeOnce(final List<Parameter> parameters) {
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
        private int batchSize = 1;

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
         * Sets a named parameter. If name is null throws a
         * {@link NullPointerException}. If value is instance of Observable then
         * throws an {@link IllegalArgumentException}.
         * 
         * @param name
         *            the parameter name. Cannot be null.
         * @param value
         *            the parameter value
         */
        public Builder parameter(String name, Object value) {
            builder.parameter(name, value);
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

        /**
         * Returns a builder used to specify how to process the generated keys
         * {@link ResultSet}. Not all jdbc drivers support this functionality
         * and some have limitations in their support (h2 for instance only
         * returns the last generated key when multiple inserts happen in the
         * one statement).
         * 
         * @return a builder used to specify how to process the generated keys
         *         ResultSet
         */
        public ReturnGeneratedKeysBuilder returnGeneratedKeys() {
            return new ReturnGeneratedKeysBuilder(builder, batchSize);
        }

        /**
         * Returns an {@link Observable} with the count of rows affected by the
         * update statement.
         * 
         * @return Observable of counts of rows affected.
         */
        public Observable<Integer> count() {
            return new QueryUpdate<Integer>(builder.sql(), builder.parameters(), builder.depends(),
                    builder.context(), null, batchSize).count();
        }

        /**
         * Executes the update query immediately, blocking till completion and
         * returns total of counts of records affected.
         * 
         * @return total of counts of records affected by update queries
         */
        public int execute() {
            return count().reduce(0, TotalHolder.TOTAL).toBlocking().single();
        }

        private static final class TotalHolder {
            static final Func2<Integer, Integer, Integer> TOTAL = new Func2<Integer, Integer, Integer>() {

                @Override
                public Integer call(Integer a, Integer b) {
                    return a + b;
                }
            };
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
         * @return operator
         */
        public Operator<Observable<Integer>, Observable<Object>> parameterListOperator() {
            return new QueryUpdateOperatorFromObservable<Object>(this);
        }

        /**
         * Clears the parameter inputs for the query.
         * 
         * @return the current builder
         */
        public Builder clearParameters() {
            builder.clearParameters();
            return this;
        }

        public Builder batchSize(int size) {
            Preconditions.checkArgument(size > 0, "size must be greater than zero");
            this.batchSize = size;
            return this;
        }
    }

    public static class ReturnGeneratedKeysBuilder {

        private final QueryBuilder builder;
        private int batchSize;

        public ReturnGeneratedKeysBuilder(QueryBuilder builder, int batchSize) {
            this.builder = builder;
            this.batchSize = batchSize;
        }

        public ReturnGeneratedKeysBuilder batchSize(int size) {
            this.batchSize = size;
            return this;
        }

        /**
         * Transforms the results using the given function.
         *
         * @param function
         * @return
         */
        public <T> Observable<T> get(ResultSetMapper<? extends T> function) {
            return QueryUpdate.get(new QueryUpdate<T>(builder.sql(), builder.parameters(),
                    builder.depends(), builder.context(), function, batchSize));
        }

        /**
         * <p>
         * Transforms each row of the {@link ResultSet} into an instance of
         * <code>T</code> using <i>automapping</i> of the ResultSet columns into
         * corresponding constructor parameters that are assignable. Beyond
         * normal assignable criteria (for example Integer 123 is assignable to
         * a Double) other conversions exist to facilitate the automapping:
         * </p>
         * <p>
         * They are:
         * <ul>
         * <li>java.sql.Blob &#10143; byte[]</li>
         * <li>java.sql.Blob &#10143; java.io.InputStream</li>
         * <li>java.sql.Clob &#10143; String</li>
         * <li>java.sql.Clob &#10143; java.io.Reader</li>
         * <li>java.sql.Date &#10143; java.util.Date</li>
         * <li>java.sql.Date &#10143; Long</li>
         * <li>java.sql.Timestamp &#10143; java.util.Date</li>
         * <li>java.sql.Timestamp &#10143; Long</li>
         * <li>java.sql.Time &#10143; java.util.Date</li>
         * <li>java.sql.Time &#10143; Long</li>
         * <li>java.math.BigInteger &#10143;
         * Short,Integer,Long,Float,Double,BigDecimal</li>
         * <li>java.math.BigDecimal &#10143;
         * Short,Integer,Long,Float,Double,BigInteger</li>
         * </p>
         * 
         * @param cls
         * @return
         */
        public <T> Observable<T> autoMap(Class<T> cls) {
            Util.setSqlFromQueryAnnotation(cls, builder);
            return get(Util.autoMap(cls));
        }

        /**
         * Automaps the first column of the ResultSet into the target class
         * <code>cls</code>.
         * 
         * @param cls
         * @return
         */
        public <T> Observable<T> getAs(Class<T> cls) {
            return get(Tuples.single(cls));
        }

        /**
         * Automaps all the columns of the {@link ResultSet} into the target
         * class <code>cls</code>. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls
         * @return
         */
        public <T> Observable<TupleN<T>> getTupleN(Class<T> cls) {
            return get(Tuples.tupleN(cls));
        }

        /**
         * Automaps all the columns of the {@link ResultSet} into {@link Object}
         * . See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls
         * @return
         */
        public Observable<TupleN<Object>> getTupleN() {
            return get(Tuples.tupleN(Object.class));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @return
         */
        public <T1, T2> Observable<Tuple2<T1, T2>> getAs(Class<T1> cls1, Class<T2> cls2) {
            return get(Tuples.tuple(cls1, cls2));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @param cls3
         * @return
         */
        public <T1, T2, T3> Observable<Tuple3<T1, T2, T3>> getAs(Class<T1> cls1, Class<T2> cls2,
                Class<T3> cls3) {
            return get(Tuples.tuple(cls1, cls2, cls3));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @param cls3
         * @param cls4
         * @return
         */
        public <T1, T2, T3, T4> Observable<Tuple4<T1, T2, T3, T4>> getAs(Class<T1> cls1,
                Class<T2> cls2, Class<T3> cls3, Class<T4> cls4) {
            return get(Tuples.tuple(cls1, cls2, cls3, cls4));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @param cls3
         * @param cls4
         * @param cls5
         * @return
         */
        public <T1, T2, T3, T4, T5> Observable<Tuple5<T1, T2, T3, T4, T5>> getAs(Class<T1> cls1,
                Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5) {
            return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @param cls3
         * @param cls4
         * @param cls5
         * @param cls6
         * @return
         */
        public <T1, T2, T3, T4, T5, T6> Observable<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(
                Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5,
                Class<T6> cls6) {
            return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
        }

        /**
         * Automaps the columns of the {@link ResultSet} into the specified
         * classes. See {@link #autoMap(Class) autoMap()}.
         * 
         * @param cls1
         * @param cls2
         * @param cls3
         * @param cls4
         * @param cls5
         * @param cls6
         * @param cls7
         * @return
         */
        public <T1, T2, T3, T4, T5, T6, T7> Observable<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(
                Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5,
                Class<T6> cls6, Class<T7> cls7) {
            return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
        }

        public Observable<Integer> count() {
            return get(Util.toOne()).count();
        }

    }

}
