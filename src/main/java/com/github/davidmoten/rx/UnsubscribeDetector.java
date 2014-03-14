package com.github.davidmoten.rx;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable.Operator;
import rx.Subscriber;
import rx.Subscription;

/**
 * Provides a {@link CountDownLatch} to assist with detecting unsubscribe calls.
 * 
 * @param <T>
 */
public class UnsubscribeDetector<T> implements Operator<T, T> {

    private static final Logger log = LoggerFactory.getLogger(UnsubscribeDetector.class);

    private final CountDownLatch latch;

    /**
     * Constructor.
     */
    public UnsubscribeDetector() {
        latch = new CountDownLatch(1);
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super T> subscriber) {
        subscriber.add(new Subscription() {

            private final AtomicBoolean subscribed = new AtomicBoolean(true);

            @Override
            public void unsubscribe() {
                latch.countDown();
                subscribed.set(false);
                log.info("unsubscribed");
            }

            @Override
            public boolean isUnsubscribed() {
                return subscribed.get();
            }
        });
        return subscriber;
    }

    /**
     * Returns a latch that will be at zero if one unsubscribe has occurred.
     * 
     * @return
     */
    public CountDownLatch latch() {
        return latch;
    }

    /**
     * Factory method.
     * 
     * @return
     */
    public static <T> UnsubscribeDetector<T> detect() {
        return new UnsubscribeDetector<T>();
    }
}
