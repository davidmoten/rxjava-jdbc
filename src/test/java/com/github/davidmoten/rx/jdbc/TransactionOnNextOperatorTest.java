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
                .compose(db.beginTransactionOnNext_())
                // update all scores to the item
                .compose(db.update("update person set score=?").parameterTransformer())
                // to empty parameter list
                .map(toEmpty())
                // increase score
                .compose(db.update("update person set score=score + 5").parameterListTransformer())
                // only expect one result so can flatten
                .compose(RxUtil.<Integer> flatten())
                // commit transaction
                .compose(db.commitOnNext_())
                // to empty lists
                .map(toEmpty())
                // return count
                .compose(db.select("select min(score) from person").dependsOnTransformer()
                        .getAs(Integer.class));
        assertIs(18, min);
    }

    static <T> void assertIs(T t, Observable<T> observable) {
        assertEquals(t, observable.toBlocking().single());
    }

}
