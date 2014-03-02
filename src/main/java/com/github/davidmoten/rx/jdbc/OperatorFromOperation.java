package com.github.davidmoten.rx.jdbc;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class OperatorFromOperation<R, T> implements Operator<R, T> {

	private final Func1<Observable<T>, Observable<R>> operation;

	public OperatorFromOperation(Func1<Observable<T>, Observable<R>> operation) {
		this.operation = operation;
	}

	@Override
	public Subscriber<? super T> call(Subscriber<? super R> subscriber) {
		final PublishSubject<T> subject = PublishSubject.create();
		operation.call(subject).subscribe(subscriber);
		Subscriber<T> result = new Subscriber<T>() {

			@Override
			public void onCompleted() {
				subject.onCompleted();
			}

			@Override
			public void onError(Throwable e) {
				subject.onError(e);
			}

			@Override
			public void onNext(T t) {
				subject.onNext(t);
			}
		};
		subscriber.add(result);
		return result;
	}
}
