package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

/**
 * Provides database connections via a JNDI lookup.
 */
public final class ConnectionProviderFromContext implements ConnectionProvider {

    private final String jndiResource;

    /**
     * Constructor.
     * 
     * @param jndiResource
     *            the name to lookup
     */
    public ConnectionProviderFromContext(String jndiResource) {
        this.jndiResource = jndiResource;
    }

    @Override
    public Connection get() {
        try {
            Context ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup(jndiResource);
            Connection conn = ds.getConnection();
            return conn;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

}
