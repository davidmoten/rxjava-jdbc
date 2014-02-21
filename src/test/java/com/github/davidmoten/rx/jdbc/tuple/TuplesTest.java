package com.github.davidmoten.rx.jdbc.tuple;

import org.junit.Test;

import com.github.davidmoten.rx.jdbc.TestingUtil;

public class TuplesTest {

	@Test
	public void testTuplesInstantiationForCoverage() {
		TestingUtil.instantiateUsingPrivateConstructor(Tuples.class);
	}
}
