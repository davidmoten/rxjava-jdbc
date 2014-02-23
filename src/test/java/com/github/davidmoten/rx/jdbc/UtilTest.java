package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.autoMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

import org.easymock.EasyMock;
import org.junit.Test;

public class UtilTest {
	@Test
	public void obtainCoverageOfPrivateConstructor() {
		TestingUtil.instantiateUsingPrivateConstructor(Util.class);
	}

	@Test
	public void testAutoMapOfUtilDateToSqlDate() {
		assertEquals(new java.sql.Date(1),
				autoMap(new java.util.Date(1), java.sql.Date.class));
	}

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

	private static class Simple {
	}

	@Test
	public void testAutoMapOfSqlDateToSimple() {
		assertEquals(new java.sql.Date(1),
				autoMap(new java.sql.Date(1), Simple.class));
	}

	@Test
	public void testAutoMapOfSqlTimestampToUtilDate() {
		assertEquals(new java.util.Date(1),
				autoMap(new java.sql.Timestamp(1), java.util.Date.class));
	}

	@Test
	public void testAutoMapOfSqlTimestampToLong() {
		assertEquals(1L, autoMap(new java.sql.Timestamp(1), Long.class));
	}

	@Test
	public void testAutoMapOfSqlTimestampToBigInteger() {
		assertEquals(BigInteger.ONE,
				autoMap(new java.sql.Timestamp(1), BigInteger.class));
	}

	@Test
	public void testAutoMapOfSqlTimestampToSimple() {
		assertEquals(new java.sql.Timestamp(1),
				autoMap(new java.sql.Timestamp(1), Simple.class));
	}

	@Test
	public void testAutoMapOfSqlTimeToUtilDate() {
		assertEquals(new java.util.Date(1),
				autoMap(new java.sql.Time(1), java.util.Date.class));
	}

	@Test
	public void testAutoMapOfSqlTimeToLong() {
		assertEquals(1L, autoMap(new java.sql.Time(1), Long.class));
	}

	@Test
	public void testAutoMapOfSqlTimeToBigInteger() {
		assertEquals(BigInteger.ONE,
				autoMap(new java.sql.Time(1), BigInteger.class));
	}

	@Test
	public void testAutoMapOfSqlTimeToSimple() {
		assertEquals(new java.sql.Time(1),
				autoMap(new java.sql.Time(1), Simple.class));
	}

	@Test
	public void testAutoMapBlobToByteArray() throws SQLException {
		Blob blob = EasyMock.createMock(Blob.class);
		byte[] b = "hello there".getBytes();
		expect(blob.getBinaryStream()).andReturn(new ByteArrayInputStream(b))
				.once();
		blob.free();
		EasyMock.expectLastCall().once();
		replay(blob);
		Object bytes = autoMap(blob, byte[].class);
		Arrays.equals(b, (byte[]) bytes);
		verify(blob);
	}

	@Test
	public void testAutoMapBlobToSimple() throws SQLException {
		Blob blob = EasyMock.createMock(Blob.class);
		replay(blob);
		Object bytes = autoMap(blob, Simple.class);
		assertEquals(bytes, blob);
		verify(blob);
	}
}
