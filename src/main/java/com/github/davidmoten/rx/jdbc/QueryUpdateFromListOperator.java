package com.github.davidmoten.rx.jdbc;

import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import com.github.davidmoten.rx.OperatorFromOperation;

/**
 * {@link Operator} corresonding to {@link QueryUpdateOperation}.
 */
public class QueryUpdateFromListOperator implements Operator<Integer, List<Object>> {

    private final OperatorFromOperation<Integer, List<Object>> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param operatorType
     */
    QueryUpdateFromListOperator(final QueryUpdate.Builder builder) {
        operator = new OperatorFromOperation<Integer, List<Object>>(
                new Func1<Observable<List<Object>>, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(Observable<List<Object>> parameterLists) {
                        return parameterLists.flatMap(new Func1<List<Object>, Observable<Integer>>() {
                            @Override
                            public Observable<Integer> call(List<Object> parameters) {
                                return builder.parameters(Observable.from(parameters)).count();
                            }
                        });
                    }
                });
    }

    @Override
    public Subscriber<? super List<Object>> call(Subscriber<? super Integer> subscriber) {
        return operator.call(subscriber);
    }
}
