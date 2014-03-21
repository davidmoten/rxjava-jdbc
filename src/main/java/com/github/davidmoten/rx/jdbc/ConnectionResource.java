package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscription;

public class ConnectionResource implements Subscription {

    private static final Logger log = LoggerFactory.getLogger(ConnectionResource.class);

    private final boolean closeOnlyIfAutoCommit;
    private final Connection con;
    private PreparedStatement ps;
    private ResultSet rs;

    public ConnectionResource(Connection con, boolean closeOnlyIfAutoCommit) {
        this.con = con;
        this.closeOnlyIfAutoCommit = closeOnlyIfAutoCommit;
    }

    @Override
    public void unsubscribe() {
        close();
    }

    @Override
    public boolean isUnsubscribed() {
        try {
            return con.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        Conditions.checkNull(ps);
        log.debug("preparing statement");
        ps = con.prepareStatement(sql);
        log.debug("prepared statement");
        return ps;
    }

    public void setParameters(List<Parameter> parameters) throws SQLException {
        Util.setParameters(ps, parameters);
    }

    public void executeQuery() throws SQLException {
        log.debug("executing ps " + ps);
        rs = ps.executeQuery();
        log.debug("executed ps");
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public ResultSet rs() {
        return rs;
    }

    /**
     * Closes connection resources (connection, prepared statement and result
     * set).
     */
    void close() {
        log.debug("closing rs");
        Util.closeQuietly(rs);
        log.debug("closing ps");
        Util.closeQuietly(ps);
        log.debug("closing con");
        if (closeOnlyIfAutoCommit)
            Util.closeQuietlyIfAutoCommit(con);
        else
            Util.closeQuietly(con);
        log.debug("closed");
    }

}
