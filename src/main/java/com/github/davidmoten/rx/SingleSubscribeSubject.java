package com.github.davidmoten.rx;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

public class SingleSubscribeSubject<T> extends Observable<T> implements
		Observer<T> {

	private final SingleSubscribeOnSubscribe<T> subscriberHolder;

	private SingleSubscribeSubject(SingleSubscribeOnSubscribe<T> onSubscribe) {
		super(onSubscribe);
		subscriberHolder = onSubscribe;
	}

	public SingleSubscribeSubject() {
		this(new SingleSubscribeOnSubscribe<T>());
	}

	@Override
	public void onCompleted() {
		if (subscriberHolder.subscriber != null)
			subscriberHolder.subscriber.onCompleted();
	}

	@Override
	public void onError(Throwable e) {
		if (subscriberHolder.subscriber != null)
			subscriberHolder.subscriber.onError(e);
	}

	@Override
	public void onNext(T t) {
		if (subscriberHolder.subscriber != null)
			subscriberHolder.subscriber.onNext(t);
	}

	private static class SingleSubscribeOnSubscribe<T> implements
			OnSubscribe<T> {

		volatile Subscriber<? super T> subscriber;

		@Override
		public void call(Subscriber<? super T> subscriber) {
			if (this.subscriber != null)
				throw new RuntimeException("only once subscription is allowed");
			this.subscriber = subscriber;
		}

	}

}