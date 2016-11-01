package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.rx.RxUtil;
import com.github.davidmoten.rx.jdbc.annotations.Column;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

public class DatabaseExampleTest {

    @Test
    public void testCreateAndUseAnInMemoryDatabase() throws SQLException {
        // create an h2 in-memory database that does not get dropped when all
        // connections closed
        Database db = Database.from("jdbc:h2:mem:demo1;DB_CLOSE_DELAY=-1");
        db.update(
                "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                .count().toBlocking().single();

        assertEquals(2, db.update("insert into person(name,score) values(?,?)")
                .parameters("FRED", 21, "JOE", 34).execute());

        // use java 8 lambdas if you have them !
        db.select("select name, score from person").autoMap(Person.class)
                .forEach(new Action1<Person>() {
                    @Override
                    public void call(Person person) {
                        System.out.println(person.name() + " has score " + person.score());
                    }
                });
        db.close();
    }

    static interface Person {
        @Column
        String name();

        @Column
        int score();
    }

    @Test
    public void testParameterOperatorBackpressure() {
        // use composition to find the first person alphabetically with
        // a score less than the person with the last name alphabetically
        // whose name is not XAVIER. Two threads and connections will be used.

        Database db = DatabaseCreator.db();
        TestSubscriber<String> ts = TestSubscriber.create(0);
        Observable.just("FRED", "FRED").repeat()
                .compose(db.select("select name from person where name = ?").parameterTransformer()
                        .getAs(String.class))
                .subscribe(ts);
        ts.requestMore(1);
        ts.assertValueCount(1);
        ts.unsubscribe();
    }

    @Test
    public void testSelectParameterListOperatorWithError() {
        // use composition to find the first person alphabetically with
        // a score less than the person with the last name alphabetically
        // whose name is not XAVIER. Two threads and connections will be used.

        Database db = DatabaseCreator.db();
        TestSubscriber<String> ts = TestSubscriber.create();
        RuntimeException ex = new RuntimeException();
        Observable.<Observable<Object>> error(ex)
                .compose(db.select("select name from person where name = ?")
                        .parameterListTransformer().getAs(String.class))
                .subscribe(ts);
        ts.assertError(ex);
    }

    @Test
    public void testUpdateParameterListOperatorWithError() {
        Database db = DatabaseCreator.db();
        TestSubscriber<Integer> ts = TestSubscriber.create();
        RuntimeException ex = new RuntimeException();
        Observable.<Observable<Object>> error(ex)
                // update
                .compose(db.update("update person set name=?||'zz' where name = ?")
                        .parameterListTransformer())
                // flatten
                .compose(RxUtil.<Integer> flatten()).subscribe(ts);
        ts.assertError(ex);
    }

    @Test
    // ignore because infinite and doesn't test results yet apart from critical
    // failure
    @Ignore
    public void testBackpressureIssue38() {
        Database db = DatabaseCreator.db();
        final Long start = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(1);
        Observable.interval(0, 1, TimeUnit.MILLISECONDS).onBackpressureLatest()
                // .doOnNext(t -> System.out.println("start " + t))
                .compose(db.select("SELECT * FROM (VALUES (?)) AS v").parameterTransformer()
                        .getAs(String.class))
                // .doOnNext(t -> System.out.println("result " + t))
                .concatMap(new Func1<String, Observable<String>>() {

                    @Override
                    public Observable<String> call(String t) {
                        return Observable.<String> empty().delay(100, TimeUnit.MILLISECONDS)
                                .startWith(t);
                    }
                }).subscribe(new Subscriber<String>() {

                    @Override
                    public void onCompleted() {
                        System.out.println("completed");
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        System.out.println("Something went wrong ");
                        e.printStackTrace();
                        latch.countDown();

                    }

                    @Override
                    public void onNext(String t) {
                        System.out.println("item " + t + " received at "
                                + (System.currentTimeMillis() - start) + "ms");

                    }
                });
        try {
            latch.await();
        } catch (InterruptedException e)
        // ignore
        {
            throw new RuntimeException(e);
        }
    }

}
