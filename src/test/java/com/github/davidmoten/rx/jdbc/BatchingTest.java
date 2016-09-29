package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class BatchingTest {

	@Test
	public void test() {
		Database db = DatabaseCreator.db();
		int numPeopleBefore = db.select("select count(*) from person") //
				.getAs(Integer.class) //
				.toBlocking().single();
		Observable<String> names = Observable.just("NANCY", "WARREN", "ALFRED", "BARRY", "ROBERTO");

		Observable<Integer> count = db.update("insert into person(name,score) values(?,0)")
				.dependsOn(db.beginTransaction())
				// set batch size
				.batchSize(3)
				// get parameters from last query
				.parameters(names)
				// go
				.count()
				// end transaction
				.count();
		assertTrue(db.commit(count).toBlocking().single());
		int numPeople = db.select("select count(*) from person") //
				.getAs(Integer.class) //
				.toBlocking().single();
		assertEquals(numPeopleBefore + 5, numPeople);
		assertNull(Batch.get().getPreparedStatement());
	}
	
	@Test
	public void testFlatMap() {
		Observable.just(1, 2, 3)
		    .doOnRequest(new Action1<Long>() {
				@Override
				public void call(Long n) {
					System.out.println(n);
				}
			})
			.flatMap(new Func1<Integer, Observable<Integer>>() {
				@Override
				public Observable<Integer> call(Integer x) {
					return Observable.just(x);
				}
			}, 1).subscribe();
	}

}
