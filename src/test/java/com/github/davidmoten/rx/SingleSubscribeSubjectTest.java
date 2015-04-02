package com.github.davidmoten.rx;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import rx.Notification;
import rx.Notification.Kind;
import rx.exceptions.OnErrorNotImplementedException;
import rx.functions.Action1;

public class SingleSubscribeSubjectTest {

    @Test
    public void testCanCallWithoutBeingSubscribed() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        subject.onNext(1);
        subject.onError(new RuntimeException());
        subject.onCompleted();
    }

    @Test
    public void testOnNextThenComplete() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        final List<Notification<Integer>> list = new ArrayList<Notification<Integer>>();
        subject.materialize().forEach(new Action1<Notification<Integer>>() {
            @Override
            public void call(Notification<Integer> n) {
                list.add(n);
            }
        });
        subject.onNext(1);
        subject.onCompleted();
        assertEquals(2, list.size());
        assertEquals(1, (int) list.get(0).getValue());
        assertEquals(Kind.OnCompleted, list.get(1).getKind());
    }

    @Test
    public void testError() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        final List<Notification<Integer>> list = new ArrayList<Notification<Integer>>();
        subject.materialize().forEach(new Action1<Notification<Integer>>() {
            @Override
            public void call(Notification<Integer> n) {
                list.add(n);
            }
        });
        subject.onNext(1);
        RuntimeException e = new RuntimeException();
        subject.onError(e);
        assertEquals(2, list.size());
        assertEquals(1, (int) list.get(0).getValue());
        assertEquals(Kind.OnError, list.get(1).getKind());
        assertEquals(e, list.get(1).getThrowable());
    }

    @Test
    public void testCanOnlySubscribeOnce() {
        SingleSubscribeSubject<Integer> subject = SingleSubscribeSubject.create();
        final List<Notification<Integer>> list = new ArrayList<Notification<Integer>>();
        subject.materialize().forEach(new Action1<Notification<Integer>>() {
            @Override
            public void call(Notification<Integer> n) {
                list.add(n);
            }
        });
        subject.materialize().forEach(new Action1<Notification<Integer>>() {
            @Override
            public void call(Notification<Integer> n) {
                list.add(n);
            }
        });
        assertEquals(1, list.size());
        assertEquals(Kind.OnError, list.get(0).getKind());
        assertEquals(SingleSubscribeSubject.ONLY_ONE_SUBSCRIPTION_IS_ALLOWED, list.get(0)
                .getThrowable().getMessage());
    }
}
