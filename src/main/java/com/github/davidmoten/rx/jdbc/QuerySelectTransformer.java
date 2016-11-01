package com.github.davidmoten.rx.jdbc;

import com.github.davidmoten.rx.jdbc.QuerySelect.Builder;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;

/**
 * Transformer corresponding to the QuerySelectOperation.
 * 
 * @param <T>
 */
final class QuerySelectTransformer<T, R> implements Transformer<R, T> {

    private final Builder builder;
    private final ResultSetMapper<? extends T> function;
    private final OperatorType operatorType;

    /**
     * Constructor.
     * 
     * @param builder
     * @param function
     * @param operatorType
     * @param resultSetTransform
     */
    QuerySelectTransformer(final QuerySelect.Builder builder,
            final ResultSetMapper<? extends T> function, final OperatorType operatorType) {
        this.builder = builder;
        this.function = function;
        this.operatorType = operatorType;
    }

    @Override
    public Observable<T> call(Observable<R> source) {
        if (operatorType == OperatorType.PARAMETER)
            return builder.parameters(source).get(function);
        else if (operatorType == OperatorType.DEPENDENCY)
            // dependency
            return builder.dependsOn(source).get(function);
        else // PARAMETER_LIST
        {
            @SuppressWarnings("unchecked")
            Observable<Observable<Object>> obs = (Observable<Observable<Object>>) source;
            return obs.concatMap(new Func1<Observable<Object>, Observable<T>>() {
                @Override
                public Observable<T> call(Observable<Object> parameters) {
                    return builder.parameters(parameters).get(function);
                }
            });
        }
    }

}
