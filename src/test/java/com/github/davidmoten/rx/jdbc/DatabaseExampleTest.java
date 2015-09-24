package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

import com.github.davidmoten.rx.jdbc.annotations.Column;

public class DatabaseExampleTest {

    @Test
    public void testCreateAndUseAnInMemoryDatabase() throws SQLException {
        // create an h2 in-memory database that does not get dropped when all
        // connections closed
        Database db = Database.from("jdbc:h2:mem:demo1;DB_CLOSE_DELAY=-1");
        db.update(
                "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                .count().toBlocking().single();

        assertEquals(
                2,
                db.update("insert into person(name,score) values(?,?)")
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
                .lift(db.select("select name from person where name = ?")
                        .parameterOperator().getAs(String.class))
                .subscribe(ts);
        ts.requestMore(1);
        ts.assertValueCount(1);
        ts.unsubscribe();
    }
    
}
