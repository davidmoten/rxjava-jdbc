package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import rx.Observable;

import java.util.Arrays;
import java.util.List;

public class BatchingTest {

    @Test
    public void test() {
        Database db = DatabaseCreator.db();
        int numPeopleBefore = db.select("select count(*) from person").getAs(Integer.class)
                .toBlocking().single();

        List<String> namesList = Arrays.asList("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO", "LENNY",
                "REBECCA","DAVID","THOMAS","JONATHAN","MILTON","PETER","JARED","ANNA",
                "JILLIAN","HEATHER","JACK","SARAH","LARRY","LLOYD","SAMUEL");

        Observable<String> names = Observable.from(namesList);

        Observable<Integer> count = db.update("insert into person(name,score) values(?,0)")
                .dependsOn(db.beginTransaction())
                // set batch size
                .batchSize(5)
                // get parameters from last query
                .parameters(names)
                .afterBatchCommit(() -> System.out.println(db.select("SELECT COUNT(*) from person").getAs(Integer.class).toBlocking().single()))
                // go
                .count()
                // end transaction
                .count();
        assertTrue(db.commit(count).toBlocking().single());
        int numPeople = db.select("select count(*) from person").getAs(Integer.class).toBlocking()
                .single();
        assertEquals(numPeopleBefore + namesList.size(), numPeople);
    }

}
