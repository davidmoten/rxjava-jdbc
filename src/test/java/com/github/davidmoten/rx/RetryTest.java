package com.github.davidmoten.rx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import rx.functions.Action1;
import rx.subjects.PublishSubject;

/**
 * Unit test for retry operator in RxJava which has shown some fragility lately.
 *
 */
public class RetryTest {

    // TODO move this test as pull request to rxjava-core.
    /**
     * This test overlaps somewhat with testSourceObservableCallsUnsubscribe()
     * but is simpler and synchronous.
     */
    @Test
    public void testRetrySubscribesAgainAfterError() {
        List<Integer> list = new ArrayList<Integer>();
        PublishSubject<Integer> subject = PublishSubject.create();
        subject
        // record item
        .doOnNext(addToList(list))
        // throw a RuntimeException
                .doOnNext(throwException())
                // retry on error
                .retry()
                // subscribe and ignore
                .subscribe();
        assertTrue(list.isEmpty());
        subject.onNext(1);
        assertEquals(Arrays.asList(1), list);
        subject.onNext(2);
        assertEquals(Arrays.asList(1, 2), list);
        subject.onNext(3);
        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    private Action1<Integer> throwException() {
        return new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                throw new RuntimeException("boo");
            }
        };
    }

    private Action1<Integer> addToList(final List<Integer> list) {
        return new Action1<Integer>() {
            @Override
            public void call(Integer n) {
                list.add(n);
            }
        };
    }
}
