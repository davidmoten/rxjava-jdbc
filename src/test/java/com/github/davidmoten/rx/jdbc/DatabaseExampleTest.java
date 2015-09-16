package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Test;

import rx.functions.Action1;

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

        assertEquals(Arrays.asList(1, 1), db.update("insert into person(name,score) values(?,?)")
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

}
