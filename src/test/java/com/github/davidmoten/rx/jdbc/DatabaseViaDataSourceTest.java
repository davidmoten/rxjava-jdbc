package com.github.davidmoten.rx.jdbc;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;

public class DatabaseViaDataSourceTest extends DatabaseTestBase {

    public DatabaseViaDataSourceTest() {
        super(false);
    }

    @Override
    Database db() {
        DataSource dataSource = initDataSource();
        return DatabaseCreator.createDatabase(dataSource);
    }

    private static DataSource initDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        String dbUrl = DatabaseCreator.nextUrl();
        dataSource.setURL(dbUrl);

        String jndiName = "jdbc/RxDS";

        try {
            Context context = new InitialContext();
            context.rebind(jndiName, dataSource);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }

        return dataSource;
    }

}
