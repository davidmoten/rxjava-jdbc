package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

public class QuerySelectOperator<T> implements Operator<T, Object> {

	private final OperatorFromOperation<T, Object> operator;

	public QuerySelectOperator(final QuerySelect.Builder builder,
			final Func1<ResultSet, T> function, final OperatorType operatorType) {
		operator = new OperatorFromOperation<T, Object>(
				new Func1<Observable<Object>, Observable<T>>() {

					@Override
					public Observable<T> call(Observable<Object> parameters) {
						if (operatorType == OperatorType.PARAMETER)
							return builder.parameters(parameters).get(function);
						else
							// dependency
							return builder.dependsOn(parameters).get(function);
					}
				});
	}

	@Override
	public Subscriber<? super Object> call(Subscriber<? super T> subscriber) {
		return operator.call(subscriber);
	}
}
