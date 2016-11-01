package com.github.davidmoten.rx.jdbc;

import com.github.davidmoten.rx.jdbc.QueryUpdate.Builder;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;

/**
 * {@link Transformer} corresonding to {@link QueryUpdateOperation}.
 */
class QueryUpdateTransformerFromObservable<R>
        implements Transformer<Observable<R>, Observable<Integer>> {

    private final Builder builder;

    /**
     * Constructor.
     * 
     * @param builder
     * @param operatorType
     */
    QueryUpdateTransformerFromObservable(final QueryUpdate.Builder builder) {
        this.builder = builder;
    }

    @Override
    public Observable<Observable<Integer>> call(Observable<Observable<R>> observable) {
        return observable.map(new Func1<Observable<R>, Observable<Integer>>() {
            @Override
            public Observable<Integer> call(Observable<R> parameters) {
                return builder.clearParameters().parameters(parameters).count();
            }
        });
    }
}
