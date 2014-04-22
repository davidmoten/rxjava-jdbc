package com.github.davidmoten.rx;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import rx.functions.Action1;
import rx.subjects.PublishSubject;

/**
 * Unit test for retry operator in RxJava which has shown some fragility lately.
 *
 */
public class RetryTest {

    // TODO move this test as pull request to rxjava-core.
    /**
     * Overlaps somewhat with testSourceObservableCallsUnsubscribe() but is
     * simpler and synchronous. This test fails against 0.16.1-0.17.4, hangs on
     * 0.17.5 and passes in 0.17.6 thanks to fix for issue #1027.
     */
    @Test
    public void testRetrySubscribesAgainAfterError() {
        Action1<Integer> record = Mockito.mock(Action1.class);
        InOrder inOrder = Mockito.inOrder(record);
        PublishSubject<Integer> subject = PublishSubject.create();
        subject
        // record item
        .doOnNext(record)
        // throw a RuntimeException
                .doOnNext(throwException())
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

    private Action1<Integer> throwException() {
        return new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                throw new RuntimeException("boo");
            }
        };
    }

}
