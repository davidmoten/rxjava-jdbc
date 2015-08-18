package com.github.davidmoten.rx.jdbc;


import org.sqlite.SQLiteConnection;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public final class RegexMappableResultSet implements ResultSet {

    private final ResultSet rs;
    private final int columnCount;
    private final String[] columnLabels;
    private final HashMap<String,Integer> cachedIndices = new HashMap<>(); //-1 means unfound

    private RegexMappableResultSet(ResultSet rs) throws SQLException {
            this.rs = rs;

            columnCount = rs.getMetaData().getColumnCount();
            columnLabels = new String[columnCount];

            for (int i=1;i<=columnCount;i++) {
                columnLabels[i-1] = rs.getMetaData().getColumnName(i);
            }
    }

    public static RegexMappableResultSet of(ResultSet rs) throws SQLException {
        return new RegexMappableResultSet(rs);
    }

    @Override
    public int findColumn(String columnRegex) throws SQLException {
            Integer foundIndex = cachedIndices.get(columnRegex);
            if (foundIndex == null) {
                    foundIndex = findColumnIndex(columnRegex);
                    cachedIndices.put(columnRegex, foundIndex);
                }
            if (foundIndex.equals(-1)) {
                throw new SQLException("Column for pattern ".concat(columnRegex).concat(" not found"));
            }
            return foundIndex + 1;
    }
    private Integer findColumnIndex(String columnRegex) {
            for (int i=0;i<columnCount;i++) {
                    if (columnLabels[i].matches(columnRegex)) {
                            return Integer.valueOf(i);
                        }
                }
            return Integer.valueOf(-1);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return rs.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return rs.isWrapperFor(iface);
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public void close() throws SQLException {
        rs.close();
    }

    public boolean wasNull() throws SQLException {
        return rs.wasNull();
    }

    public String getString(int columnIndex) throws SQLException {
        return rs.getString(columnIndex);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return rs.getBoolean(columnIndex);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return rs.getByte(columnIndex);
    }

    public short getShort(int columnIndex) throws SQLException {
        return rs.getShort(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException {
        return rs.getInt(columnIndex);
    }

    public long getLong(int columnIndex) throws SQLException {
        return rs.getLong(columnIndex);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return rs.getFloat(columnIndex);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return rs.getDouble(columnIndex);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return rs.getBigDecimal(columnIndex, scale);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return rs.getBytes(columnIndex);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return rs.getDate(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return rs.getTime(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return rs.getTimestamp(columnIndex);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return rs.getAsciiStream(columnIndex);
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return rs.getUnicodeStream(columnIndex);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return rs.getBinaryStream(columnIndex);
    }

    public String getString(String columnLabel) throws SQLException {
        return rs.getString(findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return rs.getBoolean(findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return rs.getByte(findColumn(columnLabel));
    }

    public short getShort(String columnLabel) throws SQLException {
        return rs.getShort(findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return rs.getInt(findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return rs.getLong(findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return rs.getFloat(findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return rs.getDouble(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return rs.getBigDecimal(columnLabel, scale);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return rs.getBytes(findColumn(columnLabel));
    }

    public Date getDate(String columnLabel) throws SQLException {
        return rs.getDate(findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        return rs.getTime(findColumn(columnLabel));
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return rs.getTimestamp(findColumn(columnLabel));
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return rs.getAsciiStream(findColumn(columnLabel));
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return rs.getUnicodeStream(findColumn(columnLabel));
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return rs.getBinaryStream(findColumn(columnLabel));
    }

    public SQLWarning getWarnings() throws SQLException {
        return rs.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    public String getCursorName() throws SQLException {
        return rs.getCursorName();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return rs.getMetaData();
    }

    public Object getObject(int columnIndex) throws SQLException {
        return rs.getObject(columnIndex);
    }

    public Object getObject(String columnLabel) throws SQLException {
        return rs.getObject(findColumn(columnLabel));
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return rs.getCharacterStream(columnIndex);
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return rs.getCharacterStream(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return rs.getBigDecimal(columnIndex);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return rs.getBigDecimal(findColumn(columnLabel));
    }

    public boolean isBeforeFirst() throws SQLException {
        return rs.isBeforeFirst();
    }

    public boolean isAfterLast() throws SQLException {
        return rs.isAfterLast();
    }

    public boolean isFirst() throws SQLException {
        return rs.isFirst();
    }

    public boolean isLast() throws SQLException {
        return rs.isLast();
    }

    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    public boolean first() throws SQLException {
        return rs.first();
    }

    public boolean last() throws SQLException {
        return rs.last();
    }

    public int getRow() throws SQLException {
        return rs.getRow();
    }

    public boolean absolute(int row) throws SQLException {
        return rs.absolute(row);
    }

    public boolean relative(int rows) throws SQLException {
        return rs.relative(rows);
    }

    public boolean previous() throws SQLException {
        return rs.previous();
    }

    public void setFetchDirection(int direction) throws SQLException {
        rs.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return rs.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        rs.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return rs.getFetchSize();
    }

    public int getType() throws SQLException {
        return rs.getType();
    }

    public int getConcurrency() throws SQLException {
        return rs.getConcurrency();
    }

    public boolean rowUpdated() throws SQLException {
        return rs.rowUpdated();
    }

    public boolean rowInserted() throws SQLException {
        return rs.rowInserted();
    }

    public boolean rowDeleted() throws SQLException {
        return rs.rowDeleted();
    }

    public void updateNull(int columnIndex) throws SQLException {
        rs.updateNull(columnIndex);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        rs.updateBoolean(columnIndex, x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        rs.updateByte(columnIndex, x);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        rs.updateShort(columnIndex, x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        rs.updateInt(columnIndex, x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        rs.updateLong(columnIndex, x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        rs.updateFloat(columnIndex, x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        rs.updateDouble(columnIndex, x);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        rs.updateBigDecimal(columnIndex, x);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        rs.updateString(columnIndex, x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        rs.updateBytes(columnIndex, x);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        rs.updateDate(columnIndex, x);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        rs.updateTime(columnIndex, x);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        rs.updateTimestamp(columnIndex, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        rs.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        rs.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        rs.updateCharacterStream(columnIndex, x, length);
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        rs.updateObject(columnIndex, x, scaleOrLength);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        rs.updateObject(columnIndex, x);
    }

    public void updateNull(String columnLabel) throws SQLException {
        rs.updateNull(findColumn(columnLabel));
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        rs.updateBoolean(findColumn(columnLabel), x);
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        rs.updateByte(findColumn(columnLabel), x);
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        rs.updateShort(findColumn(columnLabel), x);
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        rs.updateInt(findColumn(columnLabel), x);
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        rs.updateLong(findColumn(columnLabel), x);
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        rs.updateFloat(findColumn(columnLabel), x);
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        rs.updateDouble(findColumn(columnLabel), x);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        rs.updateBigDecimal(findColumn(columnLabel), x);
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        rs.updateString(findColumn(columnLabel), x);
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        rs.updateBytes(findColumn(columnLabel), x);
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        rs.updateDate(findColumn(columnLabel), x);
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        rs.updateTime(findColumn(columnLabel), x);
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        rs.updateTimestamp(findColumn(columnLabel), x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        rs.updateAsciiStream(findColumn(columnLabel), x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        rs.updateBinaryStream(findColumn(columnLabel), x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        rs.updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        rs.updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        rs.updateObject(findColumn(columnLabel), x);
    }

    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    public Statement getStatement() throws SQLException {
        return rs.getStatement();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return rs.getObject(columnIndex, map);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        return rs.getRef(columnIndex);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        return rs.getBlob(columnIndex);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        return rs.getClob(columnIndex);
    }

    public Array getArray(int columnIndex) throws SQLException {
        return rs.getArray(columnIndex);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return rs.getObject(findColumn(columnLabel), map);
    }

    public Ref getRef(String columnLabel) throws SQLException {
        return rs.getRef(findColumn(columnLabel));
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return rs.getBlob(findColumn(columnLabel));
    }

    public Clob getClob(String columnLabel) throws SQLException {
        return rs.getClob(findColumn(columnLabel));
    }

    public Array getArray(String columnLabel) throws SQLException {
        return rs.getArray(findColumn(columnLabel));
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return rs.getDate(columnIndex, cal);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return rs.getDate(findColumn(columnLabel), cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return rs.getTime(columnIndex, cal);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return rs.getTime(findColumn(columnLabel), cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return rs.getTimestamp(columnIndex, cal);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return rs.getTimestamp(findColumn(columnLabel), cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        return rs.getURL(columnIndex);
    }

    public URL getURL(String columnLabel) throws SQLException {
        return rs.getURL(findColumn(columnLabel));
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        rs.updateRef(columnIndex, x);
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        rs.updateRef(findColumn(columnLabel), x);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        rs.updateBlob(columnIndex, x);
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        rs.updateBlob(findColumn(columnLabel), x);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        rs.updateClob(columnIndex, x);
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        rs.updateClob(findColumn(columnLabel), x);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        rs.updateArray(columnIndex, x);
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        rs.updateArray(findColumn(columnLabel), x);
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        return rs.getRowId(columnIndex);
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return rs.getRowId(findColumn(columnLabel));
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        rs.updateRowId(columnIndex, x);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        rs.updateRowId(findColumn(columnLabel), x);
    }

    public int getHoldability() throws SQLException {
        return rs.getHoldability();
    }

    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        rs.updateNString(columnIndex, nString);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        rs.updateNString(findColumn(columnLabel), nString);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        rs.updateNClob(columnIndex, nClob);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        rs.updateNClob(findColumn(columnLabel), nClob);
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        return rs.getNClob(columnIndex);
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        return rs.getNClob(findColumn(columnLabel));
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return rs.getSQLXML(columnIndex);
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return rs.getSQLXML(findColumn(columnLabel));
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        rs.updateSQLXML(columnIndex, xmlObject);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        rs.updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    public String getNString(int columnIndex) throws SQLException {
        return rs.getNString(columnIndex);
    }

    public String getNString(String columnLabel) throws SQLException {
        return rs.getNString(findColumn(columnLabel));
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return rs.getNCharacterStream(columnIndex);
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return rs.getNCharacterStream(findColumn(columnLabel));
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        rs.updateNCharacterStream(columnIndex, x, length);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        rs.updateAsciiStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        rs.updateBinaryStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        rs.updateCharacterStream(columnIndex, x, length);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        rs.updateAsciiStream(findColumn(columnLabel), x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        rs.updateBinaryStream(findColumn(columnLabel), x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        rs.updateBlob(columnIndex, inputStream, length);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        rs.updateBlob(findColumn(columnLabel), inputStream, length);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        rs.updateClob(columnIndex, reader, length);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateClob(findColumn(columnLabel), reader, length);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        rs.updateNClob(columnIndex, reader, length);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        rs.updateNClob(findColumn(columnLabel), reader, length);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        rs.updateNCharacterStream(columnIndex, x);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        rs.updateNCharacterStream(findColumn(columnLabel), reader);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        rs.updateAsciiStream(columnIndex, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        rs.updateBinaryStream(columnIndex, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        rs.updateCharacterStream(columnIndex, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        rs.updateAsciiStream(findColumn(columnLabel), x);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        rs.updateBinaryStream(findColumn(columnLabel), x);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        rs.updateCharacterStream(findColumn(columnLabel), reader);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        rs.updateBlob(columnIndex, inputStream);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        rs.updateBlob(findColumn(columnLabel), inputStream);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        rs.updateClob(columnIndex, reader);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        rs.updateClob(findColumn(columnLabel), reader);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        rs.updateNClob(columnIndex, reader);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        rs.updateNClob(findColumn(columnLabel), reader);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return rs.getObject(columnIndex, type);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return rs.getObject(findColumn(columnLabel), type);
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        rs.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        rs.updateObject(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        rs.updateObject(columnIndex, x, targetSqlType);
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        rs.updateObject(findColumn(columnLabel), x, targetSqlType);
    }
}

