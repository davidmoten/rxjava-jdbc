package com.github.davidmoten.rx.jdbc;

import rx.Observable;

/**
 * Always emits an Observable<Integer>.
 */
public class QueryUpdate implements Query {

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

	public Observable<Integer> getCount() {
		return new QueryExecutor(this).executeUpdate();
	}

	public static class Builder {

		private final QueryBuilder builder;

		public Builder(String sql, Database db) {
			this.builder = new QueryBuilder(sql, db);
		}

		public Builder dependsOn(Observable<?> dependant) {
			builder.dependsOn(dependant);
			return this;
		}

		public Builder dependsOnLastTransaction() {
			builder.dependsOnLastTransaction();
			return this;
		}

		public <T> Builder parameters(Observable<T> more) {
			builder.parameters(more);
			return this;
		}

		public <T> Builder parameters(Object... objects) {
			builder.parameters(objects);
			return this;
		}

		public Builder parameter(Object value) {
			builder.parameter(value);
			return this;
		}

		public Observable<Integer> getCount() {
			return new QueryUpdate(builder.sql(), builder.parameters(),
					builder.depends(), builder.context()).getCount();
		}
	}
}
