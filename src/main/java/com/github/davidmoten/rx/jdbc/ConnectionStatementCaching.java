package com.github.davidmoten.rx.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.github.davidmoten.rx.jdbc.PreparedStatementBatchingRecorder.BatchListener;
import com.github.davidmoten.util.Preconditions;

//Not thread safe
class ConnectionStatementCaching implements Connection {

    private final Connection con;
    private PreparedStatementBatchingRecorder ps = null;
    private String sql = null;
    private final BatchListener listener;

    private volatile boolean batchAwaitingExecute = false;

    ConnectionStatementCaching(Connection con) {
        this.con = con;
        this.listener = new PreparedStatementBatchingRecorder.BatchListener() {

            @Override
            public void postAddBatch() {
                batchAwaitingExecute = true;
            }

            @Override
            public void postExecuteBatch() {
                batchAwaitingExecute = false;

            }

            @Override
            public void postClearBatch() {
                batchAwaitingExecute = false;
            }
        };
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return con.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return con.isWrapperFor(iface);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return con.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkSql(sql);
        if (ps == null) {
            ps = new PreparedStatementBatchingRecorder(con.prepareStatement(sql), listener);
        }
        return ps;
    }

    private void checkSql(String sql) {
        Preconditions.checkArgument(this.sql == null || this.sql.equals(sql),
                "prepared statement cached for different sql");
        this.sql = sql;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return con.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return con.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        con.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return con.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        if (batchAwaitingExecute) {
            ps.executeBatch();
            batchAwaitingExecute = false;
        }
        con.commit();
    }

    @Override
    public void rollback() throws SQLException {
        con.rollback();
    }

    @Override
    public void close() throws SQLException {
        con.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return con.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return con.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        con.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return con.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        con.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return con.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        con.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return con.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return con.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        con.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkSql(sql);
        if (ps == null) {
            ps = new PreparedStatementBatchingRecorder(
                    con.prepareStatement(sql, resultSetType, resultSetConcurrency), listener);
        }
        return ps;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return con.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        con.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        con.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return con.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return con.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return con.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        con.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        con.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkSql(sql);
        if (ps == null) {
            ps = new PreparedStatementBatchingRecorder(con.prepareStatement(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability), listener);
        }
        return ps;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        checkSql(sql);
        if (ps == null) {
            ps = new PreparedStatementBatchingRecorder(con.prepareStatement(sql, autoGeneratedKeys),
                    listener);
        }
        return ps;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return con.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        checkSql(sql);
        if (ps == null) {
            ps = new PreparedStatementBatchingRecorder(con.prepareStatement(sql, columnNames),
                    listener);
        }
        return ps;
    }

    @Override
    public Clob createClob() throws SQLException {
        return con.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return con.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return con.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return con.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return con.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        con.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        con.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return con.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return con.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return con.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return con.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        con.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return con.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        con.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        con.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return con.getNetworkTimeout();
    }
}
