package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import com.github.davidmoten.rx.jdbc.QuerySelect.Builder;

public class QuerySelectOperator<T> implements Operator<T, Parameter> {

	private final Builder builder;
	private final Func1<ResultSet, T> function;

	public QuerySelectOperator(QuerySelect.Builder builder,
			Func1<ResultSet, T> function) {
		this.builder = builder;
		this.function = function;
	}

	@Override
	public Subscriber<? super Parameter> call(Subscriber<? super T> subscriber) {

		return new Subscriber<Parameter>() {

			@Override
			public void onNext(Parameter t) {

			}

			@Override
			public void onError(Throwable e) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onCompleted() {
				// TODO Auto-generated method stub

			}
		};
	}

}
