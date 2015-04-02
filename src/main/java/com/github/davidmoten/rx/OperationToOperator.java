package com.github.davidmoten.rx;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observers.Subscribers;

/**
 * Converts an Operation (a function converting one Observable into another)
 * into an {@link Operator}.
 * 
 * @param <R>
 *            to type
 * @param <T>
 *            from type
 */
public final class OperationToOperator<R, T> implements Operator<R, T> {

    public static <R, T> Operator<R, T> toOperator(Func1<Observable<T>, Observable<R>> operation) {
        return new OperationToOperator<R, T>(operation);
    }

    /**
     * The operation to convert.
     */
    private final Func1<Observable<T>, Observable<R>> operation;

    /**
     * Constructor.
     * 
     * @param operation
     *            to be converted into {@link Operator}
     */
    public OperationToOperator(Func1<Observable<T>, Observable<R>> operation) {
        this.operation = operation;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super R> subscriber) {
        SingleSubscribeSubject<T> subject = new SingleSubscribeSubject<T>();
        Subscriber<T> result = Subscribers.from(subject);
        subscriber.add(result);
        operation.call(subject).unsafeSubscribe(subscriber);
        return result;
    }

    private static class SingleSubscribeSubject<T> extends Observable<T> implements Observer<T> {

        private final SingleSubscribeOnSubscribe<T> subscriberHolder;

        protected SingleSubscribeSubject(SingleSubscribeOnSubscribe<T> onSubscribe) {
            super(onSubscribe);
            subscriberHolder = onSubscribe;
        }

        SingleSubscribeSubject() {
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

        private static class SingleSubscribeOnSubscribe<T> implements OnSubscribe<T> {

            volatile Subscriber<? super T> subscriber;

            @Override
            public void call(Subscriber<? super T> subscriber) {
                this.subscriber = subscriber;
            }

        }
        
    }



}
