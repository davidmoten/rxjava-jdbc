package com.github.davidmoten.rx.jdbc;

import org.junit.Test;

import com.github.davidmoten.rx.Actions;
import com.github.davidmoten.rx.jdbc.DatabaseTestBase.Person;

import rx.Observable;
import rx.exceptions.OnErrorNotImplementedException;
import rx.functions.Action1;

public class ExceptionsTest {

    @Test(expected = OnErrorNotImplementedException.class)
    public void testExceptionHandlingIssue55() {
        DatabaseCreator.db().select("select name, score from person order by name") //
                .getAs(Person.class) // mistakenly replaced autoMap by getAs
                .doOnNext(Actions.println()) //
                .doOnNext(new Action1<Person>() {
                    @Override
                    public void call(Person p) {
                        System.out.println(p.getName());
                    }
                }) //
                .subscribe();
    }

    @Test(expected = OnErrorNotImplementedException.class)
    public void testSubscribeWithError() {
        Observable.error(new MyException()).subscribe();
    }

    @SuppressWarnings("serial")
    private static class MyException extends RuntimeException {

    }
}
