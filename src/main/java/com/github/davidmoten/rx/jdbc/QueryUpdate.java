package com.github.davidmoten.rx.jdbc;

import rx.Observable;

/**
 * Always emits an Observable<Integer>.
 */
public class QueryUpdate<T> implements Query<T> {

	private final String sql;
	private final Observable<Parameter> parameters;
	private final QueryContext context;
	private final Observable<?> depends;

	private QueryUpdate(String sql, Observable<Parameter> parameters,
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

	@SuppressWarnings("unchecked")
	public Observable<Integer> getCount() {
		return new QueryExecutor<Integer>((QueryUpdate<Integer>) this)
				.execute().cast(Integer.class);
	}

	public static class Builder {
		private final String sql;
		private Observable<Parameter> parameters = Observable.empty();
		private final QueryContext context;
		private Observable<?> depends = Observable.empty();
		private final Database db;

		public Builder(String sql, Database db) {
			this.sql = sql;
			this.db = db;
			this.context = db.getQueryContext();
		}

		public Builder dependsOn(Observable<?> dependant) {
			depends = Observable.concat(depends, dependant);
			return this;
		}

		public Builder dependsOnLastTransaction() {
			dependsOn(db.getLastTransactionResult());
			return this;
		}

		public <T> Builder parameters(Observable<T> more) {
			this.parameters = Observable.concat(parameters,
					more.map(Parameter.TO_PARAMETER));
			return this;
		}

		public <T> Builder parameters(Object... objects) {
			for (Object object : objects)
				parameter(object);
			return this;
		}

		public Builder parameter(Object value) {
			// TODO check on supported types
			if (value instanceof Observable)
				throw new RuntimeException(
						"use parameters() method not the parameter() method for an Observable");
			parameters = Observable.concat(parameters, Observable.from(value)
					.map(Parameter.TO_PARAMETER));
			return this;
		}

		public QueryUpdate<Integer> create() {
			return new QueryUpdate<Integer>(sql, parameters, depends, context);
		}

		public Observable<Integer> getCount() {
			return create().getCount();
		}
	}
}
