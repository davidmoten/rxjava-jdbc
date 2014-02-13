package com.github.davidmoten.rx.jdbc;

import org.junit.Test;

public class ConditionsTest {

	@Test
	public void testTrueDoesNotThrowException() {
		Conditions.checkTrue(true);
	}

	@Test(expected = RuntimeException.class)
	public void testFalseThrowsException() {
		Conditions.checkTrue(false);
	}

}
