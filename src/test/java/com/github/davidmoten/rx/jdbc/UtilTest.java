package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.autoMap;
import static com.github.davidmoten.rx.jdbc.Util.closeQuietly;
import static com.github.davidmoten.rx.jdbc.Util.closeQuietlyIfAutoCommit;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
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

	@Test
	public void testAutoMapClobToByteArray() throws SQLException {
		Clob clob = EasyMock.createMock(Clob.class);
		String s = "hello there";
		expect(clob.getCharacterStream()).andReturn(new StringReader(s)).once();
		clob.free();
		EasyMock.expectLastCall().once();
		replay(clob);
		Object string = autoMap(clob, String.class);
		assertEquals(s, string);
		verify(clob);
	}

	@Test
	public void testAutoMapClobToSimple() throws SQLException {
		Clob clob = EasyMock.createMock(Clob.class);
		replay(clob);
		Object result = autoMap(clob, Simple.class);
		assertEquals(clob, result);
		verify(clob);
	}

	@Test
	public void testAutoMapBigIntegerToLong() {
		assertEquals(1L, autoMap(BigInteger.ONE, Long.class));
	}

	@Test
	public void testAutoMapBigIntegerToDouble() {
		assertEquals(1.0, (Double) autoMap(BigInteger.ONE, Double.class),
				0.00001);
	}

	@Test
	public void testAutoMapBigIntegerToFloat() {
		assertEquals(1.0, (Float) autoMap(BigInteger.ONE, Float.class), 0.00001);
	}

	@Test
	public void testAutoMapBigIntegerToShort() {
		assertEquals((short) 1, autoMap(BigInteger.ONE, Short.class));
	}

	@Test
	public void testAutoMapBigIntegerToInteger() {
		assertEquals(1, autoMap(BigInteger.ONE, Integer.class));
	}

	@Test
	public void testAutoMapBigDecimalToDouble() {
		assertEquals(1.0, (Double) autoMap(BigDecimal.ONE, Double.class),
				0.00001);
	}

	@Test
	public void testAutoMapBigDecimalToFloat() {
		assertEquals(1.0, (Float) autoMap(BigDecimal.ONE, Float.class), 0.00001);
	}

	@Test
	public void testAutoMapBigDecimalToShort() {
		assertEquals((short) 1, autoMap(BigDecimal.ONE, Short.class));
	}

	@Test
	public void testAutoMapBigDecimalToInteger() {
		assertEquals(1, autoMap(BigDecimal.ONE, Integer.class));
	}

	@Test
	public void testAutoMapBigDecimalToLong() {
		assertEquals(1L, autoMap(BigDecimal.ONE, Long.class));
	}

	@Test
	public void testAutoMapOfBigIntegerToSimple() {
		assertEquals(BigInteger.ONE, autoMap(BigInteger.ONE, Simple.class));
	}

	@Test
	public void testAutoMapOfBigDecimalToSimple() {
		assertEquals(BigDecimal.ONE, autoMap(BigDecimal.ONE, Simple.class));
	}

	@Test
	public void testAutoMapOfDoubleToBigDecimal() {
		assertEquals(BigDecimal.ONE.doubleValue(),
				((BigDecimal) autoMap(1.0, BigDecimal.class)).doubleValue(),
				0.0001);
	}

	@Test
	public void testAutoMapOfIntegerToBigDecimal() {
		assertEquals(BigDecimal.ONE.doubleValue(),
				((BigDecimal) autoMap(1, BigDecimal.class)).doubleValue(),
				0.0001);
	}

	@Test
	public void testAutoMapOfDoubleToBigInteger() {
		assertEquals(1.2, (Double) autoMap(1.2, BigInteger.class), 0.0001);
	}

	@Test
	public void testAutoMapOfShortToBigInteger() {
		assertEquals(BigInteger.ONE, autoMap((short) 1, BigInteger.class));
	}

	@Test
	public void testAutoMapOfIntegerToBigInteger() {
		assertEquals(BigInteger.ONE, autoMap(1, BigInteger.class));
	}

	@Test
	public void testAutoMapOfLongToBigInteger() {
		assertEquals(BigInteger.ONE, autoMap(1L, BigInteger.class));
	}

	@Test
	public void testClose1() throws SQLException {
		Connection con = createMock(Connection.class);
		expect(con.isClosed()).andThrow(new SQLException());
		replay(con);
		closeQuietly(con);
		verify(con);
	}

	@Test(expected = RuntimeException.class)
	public void testClose2() throws SQLException {
		Connection con = createMock(Connection.class);
		expect(con.isClosed()).andThrow(new SQLException());
		replay(con);
		closeQuietlyIfAutoCommit(con);
		verify(con);
	}

	@Test(expected = RuntimeException.class)
	public void testCommit() throws SQLException {
		Connection con = createMock(Connection.class);
		con.commit();
		expectLastCall().andThrow(new SQLException());
		replay(con);
		Util.commit(con);
		verify(con);
	}

	@Test
	public void testCommitNullDoesNothing() {
		Util.commit(null);
	}

	@Test
	public void testRollbackNullDoesNothing() {
		Util.rollback(null);
	}

	@Test(expected = RuntimeException.class)
	public void testRollback() throws SQLException {
		Connection con = createMock(Connection.class);
		con.rollback();
		expectLastCall().andThrow(new SQLException());
		replay(con);
		Util.rollback(con);
		verify(con);
	}
}
