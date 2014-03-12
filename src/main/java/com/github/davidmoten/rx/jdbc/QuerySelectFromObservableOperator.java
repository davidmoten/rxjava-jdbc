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
public class QuerySelectFromObservableOperator<T> implements Operator<T, List<Object>> {

    private final OperatorFromOperation<T, List<Object>> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param function
     * @param operatorType
     */
    QuerySelectFromObservableOperator(final QuerySelect.Builder builder, final Func1<ResultSet, T> function) {
        operator = new OperatorFromOperation<T, List<Object>>(new Func1<Observable<List<Object>>, Observable<T>>() {
            @Override
            public Observable<T> call(Observable<List<Object>> parameterLists) {
                return parameterLists.flatMap(new Func1<List<Object>, Observable<T>>() {
                    @Override
                    public Observable<T> call(List<Object> parameters) {
                        return builder.parameters(parameters).get(function);
                    }
                });
            }
        });
    }

    @Override
    public Subscriber<? super List<Object>> call(Subscriber<? super T> subscriber) {
        return operator.call(subscriber);
    }
}
