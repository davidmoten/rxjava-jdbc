package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.OperationToOperator.toOperator;

import java.sql.ResultSet;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Operator corresponding to the QuerySelectOperation.
 * 
 * @param <T>
 */
class QuerySelectOperator<T, R> implements Operator<T, R> {

    private final Operator<T, R> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param function
     * @param operatorType
     */
    QuerySelectOperator(final QuerySelect.Builder builder, final Func1<ResultSet, T> function,
            final OperatorType operatorType) {
        operator = toOperator(new Func1<Observable<R>, Observable<T>>() {

            @Override
            public Observable<T> call(Observable<R> observable) {
                if (operatorType == OperatorType.PARAMETER)
                    return builder.parameters(observable).get(function);
                else if (operatorType == OperatorType.DEPENDENCY)
                    // dependency
                    return builder.dependsOn(observable).get(function);
                else // PARAMETER_LIST
                {
                    @SuppressWarnings("unchecked")
                    Observable<Observable<Object>> obs = (Observable<Observable<Object>>) observable;
                    return obs.flatMap(new Func1<Observable<Object>, Observable<T>>() {
                        @Override
                        public Observable<T> call(Observable<Object> parameters) {
                            return builder.parameters(parameters).get(function);
                        }
                    });
                }
            }
        });
    }

    @Override
    public Subscriber<? super R> call(Subscriber<? super T> subscriber) {
        return operator.call(subscriber);
    }
}
