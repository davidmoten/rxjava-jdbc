package com.github.davidmoten.rx.jdbc;

import com.github.davidmoten.rx.jdbc.QueryUpdate.Builder;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * {@link Transformer} corresonding to {@link QueryUpdateOperation}.
 */
final class QueryUpdateTransformer<R> implements Transformer<R, Integer> {

    private final Builder builder;
    private final OperatorType operatorType;

    /**
     * Constructor.
     * 
     * @param builder
     * @param operatorType
     */
    QueryUpdateTransformer(final QueryUpdate.Builder builder, final OperatorType operatorType) {
        this.builder = builder;
        this.operatorType = operatorType;
    }

    @Override
    public Observable<Integer> call(Observable<R> source) {
        if (operatorType == OperatorType.PARAMETER)
            return builder.parameters(source).count();
        else if (operatorType == OperatorType.DEPENDENCY)
            // dependency
            return builder.dependsOn(source).count();
        else
            throw new RuntimeException("does not handle " + operatorType);
    }
}
