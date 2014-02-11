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
 * @author dxm
 * 
 * @param <T>
 */
public class QuerySelect<T> implements Query<T> {

	private final String sql;
	private final Observable<Parameter> parameters;
	private final Func1<ResultSet, T> function;
	private final QueryContext context;
	private Observable<?> depends = Observable.empty();

	private QuerySelect(String sql, Observable<Parameter> parameters,
			Observable<?> depends, Func1<ResultSet, T> function,
			QueryContext context) {
		this.sql = sql;
		this.parameters = parameters;
		this.depends = depends;
		this.function = function;
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

	public Observable<T> execute() {
		return new QueryExecutor<T>(this).execute();
	}

	public Func1<ResultSet, T> function() {
		return function;
	}

	public static class Builder<R> {
		private final String sql;
		private Observable<Parameter> parameters = Observable.empty();
		private Observable<?> depends = Observable.empty();

		private Func1<ResultSet, R> function = new Func1<ResultSet, R>() {
			@SuppressWarnings("unchecked")
			@Override
			public R call(ResultSet rs) {
				return (R) rs;
			}
		};
		private final Database db;

		public Builder(String sql, Database db) {
			this.sql = sql;
			this.db = db;
		}

		public <T> Builder<R> parameters(Observable<T> more) {
			this.parameters = Observable.concat(parameters,
					more.map(Parameter.TO_PARAMETER));
			return this;
		}

		public Builder<R> parameters(Object... objects) {
			for (Object object : objects)
				parameter(object);
			return this;
		}

		public Builder<R> dependsOn(Observable<?> dependant) {
			depends = Observable.concat(depends, dependant);
			return this;
		}

		public Builder<R> dependsOnLastTransaction() {
			dependsOn(db.getLastTransactionResult());
			return this;
		}

		public Builder<R> parameter(Object value) {
			// TODO check on supported types?
			if (value instanceof Observable)
				throw new RuntimeException(
						"use parameters() method not the parameter() method for an Observable");
			parameters = Observable.concat(parameters, Observable.from(value)
					.map(Parameter.TO_PARAMETER));
			return this;
		}

		@SuppressWarnings("unchecked")
		public <S> Observable<S> get(Func1<ResultSet, S> function) {
			this.function = (Func1<ResultSet, R>) function;
			return (Observable<S>) get();
		}

		public <S> Observable<S> autoMap(Class<S> cls) {
			return get(Util.autoMap(cls));
		}

		public QuerySelect<R> create() {
			return new QuerySelect<R>(sql, parameters, depends, function,
					db.getQueryContext());
		}

		public Observable<R> get() {
			return create().execute();
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
