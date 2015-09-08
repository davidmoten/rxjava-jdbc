package com.github.davidmoten.rx;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.mockito.InOrder;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class RetryTest {

    /**
     * Checks in a simple and synchronous way that retry resubscribes after
     * error. This test fails against 0.16.1-0.17.4, hangs on 0.17.5 and passes
     * in 0.17.6 thanks to fix for issue #1027.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConcatMapWithRetry() {

        // record emitted values with this action
        Action1<Integer> record = mock(Action1.class);
        InOrder inOrder = inOrder(record);

        // always throw an exception with this action
        Func1<Integer, Observable<Integer>> throwException = new Func1<Integer, Observable<Integer>>() {
            @Override
            public Observable<Integer> call(Integer n) {
                return Observable.create(new OnSubscribe<Integer>() {
                    @Override
                    public void call(Subscriber<? super Integer> subscriber) {
                        subscriber.onNext(100);
                        subscriber.onError(new RuntimeException("boo"));
                    }
                });
            }
        };

        // create a retrying observable based on a PublishSubject
        PublishSubject<Integer> subject = PublishSubject.create();
        subject
                // record item
                .doOnNext(record)
                // throw a RuntimeException
                .concatMap(throwException)
                // retry on error
                .retry()
                // subscribe and ignore
                .subscribe();

        inOrder.verifyNoMoreInteractions();

        subject.onNext(1);
        inOrder.verify(record).call(1);

        subject.onNext(2);
        inOrder.verify(record).call(2);

        subject.onNext(3);
        inOrder.verify(record).call(3);

        inOrder.verifyNoMoreInteractions();
    }
}
