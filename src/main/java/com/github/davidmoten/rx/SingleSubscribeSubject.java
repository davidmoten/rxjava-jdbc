package com.github.davidmoten.rx;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

public final class SingleSubscribeSubject<T> extends Observable<T> implements
		Observer<T> {

    //Visible for testing
    static final String ONLY_ONE_SUBSCRIPTION_IS_ALLOWED = "only one subscription is allowed";
    
	private final SingleSubscribeOnSubscribe<T> subscriberHolder;

	private SingleSubscribeSubject(SingleSubscribeOnSubscribe<T> onSubscribe) {
		super(onSubscribe);
		subscriberHolder = onSubscribe;
	}

	private SingleSubscribeSubject() {
		this(new SingleSubscribeOnSubscribe<T>());
	}

	public static <T> SingleSubscribeSubject<T> create(){
	    return new SingleSubscribeSubject<T>();
	}

	@Override
	public void onCompleted() {
		if (subscriberHolder.subscriber != null) {
			subscriberHolder.subscriber.onCompleted();
		}
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
				throw new RuntimeException(ONLY_ONE_SUBSCRIPTION_IS_ALLOWED);
			this.subscriber = subscriber;
		}

	}

}