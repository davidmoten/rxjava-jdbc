package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.autoMap;
import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

public class UtilTest {

	@Test
	public void testAutoMapOfSqlDateToUtilDate() {
		assertEquals(new java.util.Date(1),
				autoMap(new java.sql.Date(1), java.util.Date.class));
	}

	@Test
	public void testAutoMapOfSqlDateToLong() {
		assertEquals(1L, autoMap(new java.sql.Date(1), Long.class));
	}

	@Test
	public void testAutoMapOfSqlDateToBigInteger() {
		assertEquals(BigInteger.ONE,
				autoMap(new java.sql.Date(1), BigInteger.class));
	}

	@Test
	public void testAutoMapOfUtilDateToSqlDate() {
		assertEquals(new java.sql.Date(1),
				autoMap(new java.util.Date(1), java.sql.Date.class));
	}

	@Test
	public void obtainCoverageOfPrivateConstructor() {
		TestingUtil.instantiateUsingPrivateConstructor(Util.class);
	}

}
