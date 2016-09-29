package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.RxUtil.toEmpty;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.davidmoten.rx.RxUtil;

import rx.Observable;

public class TransactionOnNextOperatorTest {

    @Test
    public void testBeginTransactionOnNextForThreePasses() {
        Database db = DatabaseCreator.db();
        Observable<Integer> min = Observable
                // do 3 times
                .just(11, 12, 13)
                // begin transaction for each item
                .lift(db.beginTransactionOnNextOperator())
                // update all scores to the item
                .lift(db.update("update person set score=?").parameterOperator())
                // to empty parameter list
                .map(toEmpty())
                // increase score
                .lift(db.update("update person set score=score + 5").parameterListOperator())
                // only expect one result so can flatten
                .lift(RxUtil.<Integer> flatten())
                // commit transaction
                .lift(db.commitOnNextOperator())
                // to empty lists
                .map(toEmpty())
                // return count
                .lift(db.select("select min(score) from person").dependsOnOperator()
                        .getAs(Integer.class));
        assertIs(18, min);
    }
    
    static <T> void assertIs(T t, Observable<T> observable) {
        assertEquals(t, observable.toBlocking().single());
    }

}
