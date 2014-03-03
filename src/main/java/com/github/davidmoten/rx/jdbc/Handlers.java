package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class Handlers {

    private static final Logger log = LoggerFactory.getLogger(Handlers.class);

    private final Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler;
    private final Func1<Observable<Integer>, Observable<Integer>> updateHandler;

    public Handlers(Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler,
            Func1<Observable<Integer>, Observable<Integer>> updateHandler) {
        super();
        this.selectHandler = selectHandler;
        this.updateHandler = updateHandler;
    }

    /**
     * Returns the transform to be applied to the sequence of row ResultSets
     * from a select query.
     * 
     * @return
     */
    public Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler() {
        return selectHandler;
    }

    /**
     * Returns the transform to be applied to the update count from an update
     * query.
     * 
     * @return
     */
    public Func1<Observable<Integer>, Observable<Integer>> updateHandler() {
        return updateHandler;
    }

    private static Action1<Throwable> LOG_ON_ERROR_HANDLER_ACTION = new Action1<Throwable>() {

        @Override
        public void call(Throwable t) {
            log.error(t.getMessage(), t);
        }
    };

    /**
     * Logs errors at ERROR level using slf4j and returns the sequence
     * unaltered.
     */
    public static Func1<Observable<Object>, Observable<Object>> LOG_ON_ERROR_HANDLER = new Func1<Observable<Object>, Observable<Object>>() {

        @Override
        public Observable<Object> call(Observable<Object> source) {
            return source.doOnError(LOG_ON_ERROR_HANDLER_ACTION);
        }
    };

}
