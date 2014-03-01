package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Observable.Operator;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import com.github.davidmoten.rx.jdbc.QuerySelect.Builder;

public class QuerySelectFromOperator<T> implements Operator<T, Object> {

	private final Builder builder;
	private final Func1<ResultSet, T> function;
	private final OperatorType operatorType;

	public QuerySelectFromOperator(QuerySelect.Builder builder,
			Func1<ResultSet, T> function, OperatorType operatorType) {
		this.builder = builder;
		this.function = function;
		this.operatorType = operatorType;
	}

	@Override
	public Subscriber<? super Object> call(Subscriber<? super T> subscriber) {
		final PublishSubject<Object> subject = PublishSubject.create();
		final Subscription sub;
		if (operatorType == OperatorType.PARAMETER)
			sub = builder.parameters(subject).get(function)
					.subscribe(subscriber);
		else
			// dependency
			sub = builder.dependsOn(subject).get(function)
					.subscribe(subscriber);

		Subscriber<Object> result = new Subscriber<Object>() {

			@Override
			public void onNext(Object t) {
				subject.onNext(t);
			}

			@Override
			public void onError(Throwable e) {
				subject.onError(e);
			}

			@Override
			public void onCompleted() {
				subject.onCompleted();
			}
		};
		// subscriber.add(sub);
		// result.add(sub);
		return result;
	}
}
