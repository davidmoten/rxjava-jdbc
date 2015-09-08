package com.github.davidmoten.rx.jdbc;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;

import com.github.davidmoten.rx.RxUtil;

/**
 * {@link Operator} corresonding to {@link QueryUpdateOperation}.
 */
class QueryUpdateOperatorFromObservable<R> implements Operator<Observable<Integer>, Observable<R>> {

    private final Operator<Observable<Integer>, Observable<R>> operator;

    /**
     * Constructor.
     * 
     * @param builder
     * @param operatorType
     */
    QueryUpdateOperatorFromObservable(final QueryUpdate.Builder builder) {
        operator = RxUtil.toOperator(
                new Func1<Observable<Observable<R>>, Observable<Observable<Integer>>>() {

                    @Override
                    public Observable<Observable<Integer>> call(
                            Observable<Observable<R>> observable) {

                        return observable.map(new Func1<Observable<R>, Observable<Integer>>() {
                            @Override
                            public Observable<Integer> call(Observable<R> parameters) {
                                return builder.clearParameters().parameters(parameters).count();
                            }
                        });
                    }
                });
    }

    @Override
    public Subscriber<? super Observable<R>> call(
            Subscriber<? super Observable<Integer>> subscriber) {
        return operator.call(subscriber);
    }
}
