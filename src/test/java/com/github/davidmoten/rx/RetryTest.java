package com.github.davidmoten.rx;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

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
     * This test checks in a simple and synchronous way that retry resubscribes
     * after error. This test fails against 0.16.1-0.17.4, hangs on 0.17.5 and
     * passes in 0.17.6 thanks to fix for issue #1027.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRetrySubscribesAgainAfterError() {

        // record emitted values with this action
        Action1<Integer> record = mock(Action1.class);
        InOrder inOrder = inOrder(record);

        // always throw and exception with this action
        Action1<Integer> throwException = mock(Action1.class);
        doThrow(new RuntimeException()).when(throwException).call(Mockito.anyInt());

        // create a retrying observable based on a PublishSubject
        PublishSubject<Integer> subject = PublishSubject.create();
        subject
        // record item
        .doOnNext(record)
        // throw a RuntimeException
                .doOnNext(throwException)
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
