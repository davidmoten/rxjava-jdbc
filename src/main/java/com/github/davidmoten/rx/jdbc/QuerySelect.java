package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Queries.bufferedParameters;

import java.sql.ResultSet;
import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.functions.Func1;

import com.github.davidmoten.rx.jdbc.tuple.Tuple2;
import com.github.davidmoten.rx.jdbc.tuple.Tuple3;
import com.github.davidmoten.rx.jdbc.tuple.Tuple4;
import com.github.davidmoten.rx.jdbc.tuple.Tuple5;
import com.github.davidmoten.rx.jdbc.tuple.Tuple6;
import com.github.davidmoten.rx.jdbc.tuple.Tuple7;
import com.github.davidmoten.rx.jdbc.tuple.TupleN;
import com.github.davidmoten.rx.jdbc.tuple.Tuples;

/**
 * A query and its executable context.
 */
final public class QuerySelect implements Query {

	private final String sql;
	private final Observable<Parameter> parameters;
	private final QueryContext context;
	private Observable<?> depends = Observable.empty();

	/**
	 * Constructor.
	 * 
	 * @param sql
	 * @param parameters
	 * @param depends
	 * @param context
	 */
	private QuerySelect(String sql, Observable<Parameter> parameters,
			Observable<?> depends, QueryContext context) {
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
	public QueryContext context() {
		return context;
	}

	@Override
	public Observable<Parameter> parameters() {
		return parameters;
	}

	@Override
	public String toString() {
		return "QuerySelect [sql=" + sql + "]";
	}

	@Override
	public Observable<?> depends() {
		return depends;
	}

	/**
	 * Returns the results of running a select query with all sets of
	 * parameters.
	 * 
	 * @return
	 */
	public Observable<ResultSet> execute() {
		return context.handlers().selectHandler()
				.call(bufferedParameters(this).flatMap(executeOnce()));
	}

	/**
	 * Returns a {@link Func1} that itself returns the results of pushing one
	 * set of parameters through a select query.
	 * 
	 * @param query
	 * @return
	 */
	private Func1<List<Parameter>, Observable<ResultSet>> executeOnce() {
		return new Func1<List<Parameter>, Observable<ResultSet>>() {
			@Override
			public Observable<ResultSet> call(List<Parameter> params) {
				return executeOnce(params);
			}
		};
	}

	/**
	 * Returns an Observable of the results of pushing one set of parameters
	 * through a select query.
	 * 
	 * @param params
	 *            one set of parameters to be run with the query
	 * @return
	 */
	private Observable<ResultSet> executeOnce(final List<Parameter> params) {
		return QuerySelectOperation.execute(this, params).subscribeOn(
				context.scheduler());
	}

	/**
	 * Builds a {@link QuerySelect}.
	 */
	final public static class Builder {

		/**
		 * Builds the standard stuff.
		 */
		private final QueryBuilder builder;

		/**
		 * Constructor.
		 * 
		 * @param sql
		 * @param db
		 */
		public Builder(String sql, Database db) {
			builder = new QueryBuilder(sql, db);
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
			builder.parameters(parameters.lift(UnsubscribeDetector
					.<Object> detect()));
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
		 * Transforms the results using the given function.
		 * 
		 * @param function
		 * @return
		 */
		public <T> Observable<T> get(Func1<ResultSet, T> function) {
			return get().map(function);
		}

		/**
		 * <p>
		 * Transforms each row of the ResultSet into an instance of
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
			return get(Util.autoMap(cls));
		}

		/**
		 * Returns the ResultSet rows of the select query.
		 * 
		 * @return
		 */
		public Observable<ResultSet> get() {
			return new QuerySelect(builder.sql(), builder.parameters(),
					builder.depends(), builder.context()).execute();
		}

		/**
		 * Automaps the first column of the ResultSet into the target class
		 * <code>cls</code>.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Observable<S> getAs(Class<S> cls) {
			return get(Tuples.single(cls));
		}

		/**
		 * Automaps all the columns of the {@link ResultSet} into the target
		 * class <code>cls</code>. See {@link #autoMap(Class) autoMap()}.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Observable<TupleN<S>> getTupleN(Class<S> cls) {
			return get(Tuples.tupleN(cls));
		}

		/**
		 * Automaps all the columns of the {@link ResultSet} into {@link Object}
		 * . See {@link #autoMap(Class) autoMap()}.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Observable<TupleN<Object>> getTupleN() {
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
		public <T1, T2> Observable<Tuple2<T1, T2>> getAs(Class<T1> cls1,
				Class<T2> cls2) {
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
		public <T1, T2, T3> Observable<Tuple3<T1, T2, T3>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
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
		public <T1, T2, T3, T4> Observable<Tuple4<T1, T2, T3, T4>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4) {
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
		public <T1, T2, T3, T4, T5> Observable<Tuple5<T1, T2, T3, T4, T5>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5) {
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
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6) {
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
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6, Class<T7> cls7) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
		}

		public OperatorBuilder parameterOperator() {
			return new OperatorBuilder(this, OperatorType.PARAMETER);
		}

		public OperatorBuilder dependencyOperator() {
			return new OperatorBuilder(this, OperatorType.DEPENDENCY);
		}
	}

	public static class OperatorBuilder {

		private final Builder builder;
		private final OperatorType operatorType;

		public OperatorBuilder(Builder builder, OperatorType operatorType) {
			this.builder = builder;
			this.operatorType = operatorType;
		}

		/**
		 * Transforms the results using the given function.
		 * 
		 * @param function
		 * @return
		 */
		public <T> Operator<T, Object> get(Func1<ResultSet, T> function) {
			return new QuerySelectFromOperator<T>(builder, function,
					operatorType);
		}

		/**
		 * Automaps the first column of the ResultSet into the target class
		 * <code>cls</code>.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Operator<S, Object> getAs(Class<S> cls) {
			return get(Tuples.single(cls));
		}

		/**
		 * Automaps all the columns of the {@link ResultSet} into the target
		 * class <code>cls</code>. See {@link #autoMap(Class) autoMap()}.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Operator<TupleN<S>, Object> getTupleN(Class<S> cls) {
			return get(Tuples.tupleN(cls));
		}

		/**
		 * Automaps all the columns of the {@link ResultSet} into {@link Object}
		 * . See {@link #autoMap(Class) autoMap()}.
		 * 
		 * @param cls
		 * @return
		 */
		public <S> Operator<TupleN<Object>, Object> getTupleN() {
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
		public <T1, T2> Operator<Tuple2<T1, T2>, Object> getAs(Class<T1> cls1,
				Class<T2> cls2) {
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
		public <T1, T2, T3> Operator<Tuple3<T1, T2, T3>, Object> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
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
		public <T1, T2, T3, T4> Operator<Tuple4<T1, T2, T3, T4>, Object> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4) {
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
		public <T1, T2, T3, T4, T5> Operator<Tuple5<T1, T2, T3, T4, T5>, Object> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5) {
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
		public <T1, T2, T3, T4, T5, T6> Operator<Tuple6<T1, T2, T3, T4, T5, T6>, Object> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6) {
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
		public <T1, T2, T3, T4, T5, T6, T7> Operator<Tuple7<T1, T2, T3, T4, T5, T6, T7>, Object> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6, Class<T7> cls7) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
		}

	}

}
