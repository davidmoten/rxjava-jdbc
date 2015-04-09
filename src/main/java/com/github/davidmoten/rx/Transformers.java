package com.github.davidmoten.rx;

import rx.Observable.Operator;
import rx.Observable.Transformer;

public final class Transformers {

    public static <T,R> Operator<R,T> toOperator(Transformer<T,R> transformer) {
        return TransformerOperator.toOperator(transformer);
    }
    
}
