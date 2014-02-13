package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UtilTest {

	@Test
	public void testAutoMapOfDate() {
		assertEquals(new java.util.Date(1),
				Util.autoMap(new java.sql.Date(1), java.util.Date.class));
	}

	@Test
	public void obtainCoverageOfPrivateConstructor() {
		TestingUtil.instantiateUsingPrivateConstructor(Util.class);
	}

}
