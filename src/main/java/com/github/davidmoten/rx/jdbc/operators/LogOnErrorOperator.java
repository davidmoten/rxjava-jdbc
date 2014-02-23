package com.github.davidmoten.rx.jdbc.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;

public class LogOnErrorOperator {

	private static Logger log = LoggerFactory
			.getLogger(LogOnErrorOperator.class);

	public static <T> Observable<T> logOnError(Observable<T> source) {
		return Observable.create(createLogOnErrorOnSubscribeFunc(source));
	}

	private static <T> OnSubscribeFunc<T> createLogOnErrorOnSubscribeFunc(
			final Observable<T> source) {
		return new OnSubscribeFunc<T>() {

			@Override
			public Subscription onSubscribe(final Observer<? super T> o) {
				final Subscription sub = source.subscribe(new Observer<T>() {

					@Override
					public void onCompleted() {
						o.onCompleted();
					}

					@Override
					public void onError(Throwable e) {
						log.error(e.getMessage(), e);
						o.onError(e);
					}

					@Override
					public void onNext(T args) {
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
