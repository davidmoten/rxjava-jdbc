package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import rx.Observable;
import rx.util.functions.Func1;

import com.github.davidmoten.rx.jdbc.operators.LogOnErrorOperator;

public class Handlers {

	private final Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler;
	private final Func1<Observable<Integer>, Observable<Integer>> updateHandler;

	public Handlers(
			Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler,
			Func1<Observable<Integer>, Observable<Integer>> updateHandler) {
		super();
		this.selectHandler = selectHandler;
		this.updateHandler = updateHandler;
	}

	public Func1<Observable<ResultSet>, Observable<ResultSet>> selectHandler() {
		return selectHandler;
	}

	public Func1<Observable<Integer>, Observable<Integer>> updateHandler() {
		return updateHandler;
	}

	public static Func1<Observable<Object>, Observable<Object>> LOG_ON_ERROR_HANDLER = new Func1<Observable<Object>, Observable<Object>>() {

		@Override
		public Observable<Object> call(Observable<Object> source) {
			return LogOnErrorOperator.logOnError(source);
		}
	};

}
