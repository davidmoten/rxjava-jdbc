package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import com.github.davidmoten.rx.OperatorFromOperation;

/**
 * Operator corresponding to the QuerySelectOperation.
 * 
 * @param <T>
 */
public class QuerySelectOperator<T,R> implements Operator<T, R> {

    private final OperatorFromOperation<T, R> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param function
     * @param operatorType
     */
    QuerySelectOperator(final QuerySelect.Builder builder, final Func1<ResultSet, T> function,
            final OperatorType operatorType) {
        operator = new OperatorFromOperation<T, R>(new Func1<Observable<R>, Observable<T>>() {

            @Override
            public Observable<T> call(Observable<R> observable) {
                if (operatorType == OperatorType.PARAMETER)
                    return builder.parameters(observable).get(function);
                else if (operatorType==OperatorType.DEPENDENCY)
                    // dependency
                    return builder.dependsOn(observable).get(function);
                else //PARAMETER_LIST
                	return observable.cast(List.class).flatMap(new Func1<List,Observable<T>>(){
						@Override
						public Observable<T> call(List parameters) {
							return builder.parameters(Observable.from(parameters)).get(function);
						}});
            }
        });
    }

    @Override
    public Subscriber<? super R> call(Subscriber<? super T> subscriber) {
        return operator.call(subscriber);
    }
}
