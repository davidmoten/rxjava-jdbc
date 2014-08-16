package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.mockito.Mockito;

public class ConnectionNonClosingTest {

	@Test
	public void testClose() throws SQLException {
		Connection con = mock(Connection.class);
		ConnectionNonClosing c = new ConnectionNonClosing(con);
		assertFalse(c.isClosed());
		c.close();
		verifyZeroInteractions(con);
		c.clearWarnings();
		verify(con).clearWarnings();
		c.commit();
		verify(con).commit();
		c.createArrayOf(null, null);
		verify(con).createArrayOf(null, null);
		c.createBlob();
		verify(con).createBlob();
		c.createClob();
		verify(con).createClob();
		c.createNClob();
		verify(con).createNClob();
		c.createSQLXML();
		verify(con).createSQLXML();
		c.createStatement();
		verify(con).createStatement();
		c.createStatement(0, 0);
		verify(con).createStatement(0, 0);
		c.createStatement(0, 0, 0);
		verify(con).createStatement(0, 0, 0);
		c.createStruct(null, null);
		verify(con).createStruct(null, null);
		c.getAutoCommit();
		verify(con).getAutoCommit();
		c.getCatalog();
		verify(con).getCatalog();
		c.getClientInfo();
		verify(con).getClientInfo();
		c.getClientInfo(null);
		verify(con).getClientInfo(null);
		c.getHoldability();
		verify(con).getHoldability();
		c.getMetaData();
		verify(con).getMetaData();
		c.getNetworkTimeout();
		verify(con).getNetworkTimeout();
		c.getSchema();
		verify(con).getSchema();
		c.getTransactionIsolation();
		verify(con).getTransactionIsolation();
		c.getTypeMap();
		verify(con).getTypeMap();
		c.getWarnings();
		verify(con).getWarnings();
		assertTrue(c.isClosed());
		c.isReadOnly();
		verify(con).isReadOnly();
		c.isValid(0);
		verify(con).isValid(0);
		c.isWrapperFor(null);
		verify(con).isWrapperFor(null);
		c.nativeSQL(null);
		verify(con).nativeSQL(null);
		c.prepareCall(null);
		verify(con).prepareCall(null);
		c.prepareCall(null, 0, 0);
		verify(con).prepareCall(null, 0, 0);
		c.prepareCall(null, 0, 0, 0);
		verify(con).prepareCall(null, 0, 0, 0);
		c.prepareStatement(null);
		verify(con).prepareStatement(null);
		c.prepareStatement(null, 0);
		verify(con).prepareStatement(null, 0);
		c.prepareStatement(null, new int[] {});
		verify(con).prepareStatement(null, new int[] {});
		c.prepareStatement(null, new String[] {});
		verify(con).prepareStatement(null, new String[] {});
	}

}
