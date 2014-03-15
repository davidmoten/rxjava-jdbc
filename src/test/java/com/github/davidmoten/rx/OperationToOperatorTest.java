package com.github.davidmoten.rx;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Observable;
import rx.functions.Functions;

public class OperationToOperatorTest {

    @Test
    public void testUnsubscribeFromAsynchronousSource() throws InterruptedException {

        UnsubscribeDetector<Long> detector = UnsubscribeDetector.detect();
        Observable.interval(100, TimeUnit.MILLISECONDS).lift(detector)
                .lift(new OperationToOperator<Long, Long>(Functions.<Observable<Long>> identity())).take(1).first()
                .toBlockingObservable().single();
        assertTrue(detector.latch().await(1, TimeUnit.SECONDS));

    }

    @Test
    public void testMultipleSubscriptions() {

    }

}
