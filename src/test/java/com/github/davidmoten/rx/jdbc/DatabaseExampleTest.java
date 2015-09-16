package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.github.davidmoten.rx.jdbc.annotations.Column;

import rx.functions.Action1;

public class DatabaseExampleTest {

    @Test
    public void testCreateAndUseAnInMemoryDatabase() throws SQLException {
        // little trick for an in-memory h2 database so that closing a
        // connection to it doesn't throw the db away
        Connection con = Database.from("jdbc:h2:mem:demo1").getConnectionProvider().get();
        Database db = Database.from(con);
        db.update(
                "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                .count().toBlocking().single();

        db.update("insert into person(name,score) values(?,?)").parameters("FRED", 21).count()
                .toBlocking().single();

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
