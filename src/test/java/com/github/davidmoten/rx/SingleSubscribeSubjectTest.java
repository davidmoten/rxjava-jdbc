package com.github.davidmoten.rx;

import org.junit.Test;

import rx.exceptions.OnErrorNotImplementedException;

public class SingleSubscribeSubjectTest {

    @Test
    public void testCanCallWithoutBeingSubscribed() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        subject.onNext(1);
        subject.onError(new RuntimeException());
        subject.onCompleted();
    }
    
    @Test
    public void test() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        subject.subscribe();
        subject.onNext(1);
        subject.onCompleted();
    }
    
    @Test(expected=OnErrorNotImplementedException.class)
    public void testError() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        subject.subscribe();
        subject.onError(new RuntimeException());
    }
}
