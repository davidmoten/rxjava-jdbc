package com.github.davidmoten.rx;

import static com.github.davidmoten.rx.OperationToOperator.toOperator;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Functions;
import rx.subjects.PublishSubject;

public class OperationToOperatorTest {

    private static final int AWAIT_SECONDS = 10;

    @Test
    public void testUnsubscribeFromAsynchronousSource() throws InterruptedException {

        UnsubscribeDetector<Long> detector = UnsubscribeDetector.detect();
        Observable
        // every 100ms
                .interval(100, TimeUnit.MILLISECONDS)
                // detect unsubscribe
                .lift(detector)
                // use toOperator
                .lift(toOperator(Functions.<Observable<Long>> identity())).take(1).first()
                // block and get result
                .toBlockingObservable().single();
        // wait for expected unsubscription
        assertTrue(detector.latch().await(AWAIT_SECONDS, TimeUnit.SECONDS));

    }

    @Test
    public void testUnsubscribeFromSynchronousSource() throws InterruptedException {
        UnsubscribeDetector<Integer> detector = UnsubscribeDetector.detect();
        PublishSubject<Integer> subject = PublishSubject.create();
        subject
        // detect unsubscribe
        .lift(detector)
        // use toOperator
                .lift(toOperator(Functions.<Observable<Integer>> identity()))
                // get first only
                .take(1)
                // subscribe and ignore events
                .subscribe();
        subject.onNext(1);
        //should have unsubscribed because of take(1)
        assertTrue(detector.latch().await(AWAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleNonSimultaeousSubscriptions() {
        Observable<Integer> sequence = Observable.range(1, 3).lift(
                toOperator(Functions.<Observable<Integer>> identity()));
        assertEquals(asList(1, 2, 3), sequence.toList().toBlockingObservable().single());
        assertEquals(asList(1, 2, 3), sequence.toList().toBlockingObservable().single());
    }

    @Test
    public void testMultipleSimultaneousSubscriptions() {
        PublishSubject<Integer> subject = PublishSubject.create();
        Recorder recorder1 = new Recorder();
        Recorder recorder2 = new Recorder();
        subject.subscribe(recorder1);
        subject.subscribe(recorder2);
        subject.onNext(1);
        assertEquals(asList(1), recorder1.list());
        assertEquals(asList(1), recorder2.list());
        subject.onNext(2);
        assertEquals(asList(1, 2), recorder1.list());
        assertEquals(asList(1, 2), recorder2.list());
        subject.onCompleted();
    }

    private static class Recorder implements Action1<Integer> {

        private final List<Integer> list = new ArrayList<Integer>();

        @Override
        public void call(Integer t) {
            list.add(t);
        }

        List<Integer> list() {
            return list;
        }

    }

}
