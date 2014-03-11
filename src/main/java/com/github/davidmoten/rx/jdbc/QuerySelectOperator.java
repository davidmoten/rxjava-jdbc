package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

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
public class QuerySelectOperator<T> implements Operator<T, Object> {

    private final OperatorFromOperation<T, Object> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param function
     * @param operatorType
     */
    QuerySelectOperator(final QuerySelect.Builder builder, final Func1<ResultSet, T> function,
            final OperatorType operatorType) {
        operator = new OperatorFromOperation<T, Object>(new Func1<Observable<Object>, Observable<T>>() {

            @Override
            public Observable<T> call(Observable<Object> observable) {
                if (operatorType == OperatorType.PARAMETER)
                    return builder.parameters(observable).get(function);
                else
                    // dependency
                    return builder.dependsOn(observable).get(function);
            }
        });
    }

    @Override
    public Subscriber<? super Object> call(Subscriber<? super T> subscriber) {
        return operator.call(subscriber);
    }
}
