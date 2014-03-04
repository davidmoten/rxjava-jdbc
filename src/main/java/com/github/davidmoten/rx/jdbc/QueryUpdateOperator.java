package com.github.davidmoten.rx.jdbc;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import com.github.davidmoten.rx.OperatorFromOperation;

/**
 * {@link Operator} corresonding to {@link QueryUpdateOperation}.
 */
public class QueryUpdateOperator implements Operator<Integer, Object> {

	private final OperatorFromOperation<Integer, Object> operator;

	/**
	 * Constructor.
	 * 
	 * @param builder
	 * @param operatorType
	 */
	QueryUpdateOperator(final QueryUpdate.Builder builder,
			final OperatorType operatorType) {
		operator = new OperatorFromOperation<Integer, Object>(
				new Func1<Observable<Object>, Observable<Integer>>() {

					@Override
					public Observable<Integer> call(
							Observable<Object> parameters) {
						if (operatorType == OperatorType.PARAMETER)
							return builder.parameters(parameters).count();
						else
							// dependency
							return builder.dependsOn(parameters).count();
					}
				});
	}

	@Override
	public Subscriber<? super Object> call(
			Subscriber<? super Integer> subscriber) {
		return operator.call(subscriber);
	}
}
