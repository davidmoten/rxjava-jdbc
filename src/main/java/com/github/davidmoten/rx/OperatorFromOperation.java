package com.github.davidmoten.rx;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

/**
 * Converts an Operation (a function converting one Observable into another)
 * into an {@link Operator}.
 * 
 * @param <R>
 *            to type
 * @param <T>
 *            from type
 */
public class OperatorFromOperation<R, T> implements Operator<R, T> {

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
    public OperatorFromOperation(Func1<Observable<T>, Observable<R>> operation) {
        this.operation = operation;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super R> subscriber) {
        final PublishSubject<T> subject = PublishSubject.create();
        operation.call(subject).subscribe(subscriber);
        Subscriber<T> result = createSubscriber(subject);
        subscriber.add(result);
        return result;
    }

    /**
     * Creates a subscriber that passes all events on to the subject.
     * 
     * @param subject
     *            receives all events.
     * @return
     */
    private static <T> Subscriber<T> createSubscriber(final PublishSubject<T> subject) {
        return new Subscriber<T>() {

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
    }
}
