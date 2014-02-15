package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.logging.Level;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

public class Handlers {

	private static final java.util.logging.Logger log = java.util.logging.Logger
			.getLogger(Handlers.class.getName());

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

	public static Func1<Observable<Object>, Observable<Object>> utilLoggingOnErrorLoggerHandler() {
		return new Func1<Observable<Object>, Observable<Object>>() {

			@Override
			public Observable<Object> call(Observable<Object> source) {
				return Observable
						.create(createLogOnErrorOnSubscribeFunc(source));
			}

		};
	}

	private static OnSubscribeFunc<Object> createLogOnErrorOnSubscribeFunc(
			final Observable<Object> source) {
		return new OnSubscribeFunc<Object>() {

			@Override
			public Subscription onSubscribe(final Observer<? super Object> o) {
				final Subscription sub = source
						.subscribe(new Observer<Object>() {

							@Override
							public void onCompleted() {
								o.onCompleted();
							}

							@Override
							public void onError(Throwable e) {
								log.log(Level.SEVERE, e.getMessage(), e);
								o.onError(e);
							}

							@Override
							public void onNext(Object args) {
								o.onNext(args);
							}
						});
				return new Subscription() {
					@Override
					public void unsubscribe() {
						sub.unsubscribe();
					}
				};
			}
		};
	}
}
