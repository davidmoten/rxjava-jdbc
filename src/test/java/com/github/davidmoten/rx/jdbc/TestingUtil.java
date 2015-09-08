package com.github.davidmoten.rx.jdbc;

import java.util.concurrent.CountDownLatch;

import rx.functions.Action0;

public final class TestingUtil {

    public static Action0 countDown(final CountDownLatch latch) {
        return new Action0() {

            @Override
            public void call() {
                latch.countDown();
            }
        };
    }
}
