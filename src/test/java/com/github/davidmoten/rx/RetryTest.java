package com.github.davidmoten.rx;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void test() {
        final List<Integer> list = new ArrayList<Integer>();
        PublishSubject<Integer> subject = PublishSubject.create();
        subject.doOnNext(new Action1<Integer>() {
            @Override
            public void call(Integer n) {
                list.add(n);
            }
        }).doOnNext(new Action1<Integer>() {
            @Override
            public void call(Integer t1) {
                throw new RuntimeException("boo");
            }
        }).retry().subscribe();
        subject.onNext(1);
        subject.onNext(2);
        subject.onNext(3);
        assertEquals(Arrays.asList(1, 2, 3), list);
    }
}
