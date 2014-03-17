package com.github.davidmoten.rx.jdbc;

import rx.Observable.Operator;

/**
 * The types of {@link Operator} that can be created by a select or update
 * query.
 */
enum OperatorType {
    /**
     * A type of operator that consumes an Observable<Object> to be used as
     * query parameters.
     */
    PARAMETER,

    /**
     * A type of operator that consumes an Observable<Object> representing
     * dependencies to be completed before the select or update query.
     */
    DEPENDENCY,

    PARAMETER_LIST;

}
