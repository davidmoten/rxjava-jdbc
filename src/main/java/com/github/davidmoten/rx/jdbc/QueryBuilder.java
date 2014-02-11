package com.github.davidmoten.rx.jdbc;

import rx.Observable;

public class QueryBuilder {
	private final String sql;
	private Observable<Parameter> parameters = Observable.empty();
	private Observable<?> depends = Observable.empty();
	private final Database db;
	private final QueryContext context;

	public QueryBuilder(String sql, Database db) {
		this.sql = sql;
		this.db = db;
		this.context = db.getQueryContext();
	}

	public <T> void parameters(Observable<T> more) {
		this.parameters = Observable.concat(parameters,
				more.map(Parameter.TO_PARAMETER));
	}

	public void parameters(Object... objects) {
		for (Object object : objects)
			parameter(object);
	}

	public void dependsOn(Observable<?> dependant) {
		depends = Observable.concat(depends, dependant);
	}

	public void dependsOnLastTransaction() {
		dependsOn(db.getLastTransactionResult());
	}

	public void parameter(Object value) {
		// TODO check on supported types?
		if (value instanceof Observable)
			throw new RuntimeException(
					"use parameters() method not the parameter() method for an Observable");
		parameters = Observable.concat(parameters,
				Observable.from(value).map(Parameter.TO_PARAMETER));
	}

	public String sql() {
		return sql;
	}

	public Observable<Parameter> parameters() {
		return parameters;
	}

	public Observable<?> depends() {
		return depends;
	}

	public QueryContext context() {
		return context;
	}
}
