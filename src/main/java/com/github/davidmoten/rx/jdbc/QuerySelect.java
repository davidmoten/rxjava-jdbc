package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Observable;
import rx.util.functions.Func1;

import com.github.davidmoten.rx.jdbc.tuple.Tuple2;
import com.github.davidmoten.rx.jdbc.tuple.Tuple3;
import com.github.davidmoten.rx.jdbc.tuple.Tuple4;
import com.github.davidmoten.rx.jdbc.tuple.Tuple5;
import com.github.davidmoten.rx.jdbc.tuple.Tuple6;
import com.github.davidmoten.rx.jdbc.tuple.Tuple7;
import com.github.davidmoten.rx.jdbc.tuple.TupleN;
import com.github.davidmoten.rx.jdbc.tuple.Tuples;

/**
 * 
 * @param <T>
 */
public class QuerySelect implements Query {

	private final String sql;
	private final Observable<Parameter> parameters;
	private final QueryContext context;
	private Observable<?> depends = Observable.empty();

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

	public Observable<ResultSet> execute() {
		return new QueryExecutor(this).execute();
	}

	public static class Builder {

		private final QueryBuilder builder;

		public Builder(String sql, Database db) {
			builder = new QueryBuilder(sql, db);
		}

		public <T> Builder parameters(Observable<T> more) {
			builder.parameters(more);
			return this;
		}

		public Builder parameters(Object... objects) {
			builder.parameters(objects);
			return this;
		}

		public Builder parameter(Object value) {
			builder.parameter(value);
			return this;
		}

		public Builder dependsOn(Observable<?> dependant) {
			builder.dependsOn(dependant);
			return this;
		}

		public Builder dependsOnLastTransaction() {
			builder.dependsOnLastTransaction();
			return this;
		}

		public <S> Observable<S> get(Func1<ResultSet, S> function) {
			return get().map(function);
		}

		public <S> Observable<S> autoMap(Class<S> cls) {
			return get(Util.autoMap(cls));
		}

		public Observable<ResultSet> get() {
			return new QuerySelect(builder.sql(), builder.parameters(),
					builder.depends(), builder.context()).execute();
		}

		public <S> Observable<S> getAs(Class<S> cls) {
			return get(Tuples.single(cls));
		}

		public <S> Observable<TupleN<S>> getTupleN(Class<S> cls) {
			return get(Tuples.tupleN(cls));
		}

		public <T1, T2> Observable<Tuple2<T1, T2>> getAs(Class<T1> cls1,
				Class<T2> cls2) {
			return get(Tuples.tuple(cls1, cls2));
		}

		public <T1, T2, T3> Observable<Tuple3<T1, T2, T3>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
			return get(Tuples.tuple(cls1, cls2, cls3));
		}

		public <T1, T2, T3, T4> Observable<Tuple4<T1, T2, T3, T4>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4));
		}

		public <T1, T2, T3, T4, T5> Observable<Tuple5<T1, T2, T3, T4, T5>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
		}

		public <T1, T2, T3, T4, T5, T6> Observable<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
		}

		public <T1, T2, T3, T4, T5, T6, T7> Observable<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(
				Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4,
				Class<T5> cls5, Class<T6> cls6, Class<T7> cls7) {
			return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
		}
	}

}
