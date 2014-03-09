package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseCreator {

    private static AtomicInteger dbNumber = new AtomicInteger();

    public static String nextUrl() {
        return "jdbc:h2:mem:test" + dbNumber.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    public static ConnectionProvider connectionProvider() {
        return new ConnectionProviderFromUrl(nextUrl());
    }

    public static Database db() {
        ConnectionProvider cp = connectionProvider();
        Connection con = cp.get();
        createDatabase(con);
        try {
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new Database(cp);
    }

    public static Database createDatabase(ConnectionProvider cp) {
        Database db = new Database(cp);
        Connection con = cp.get();
        createDatabase(con);
        try {
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return db;
    }

    public static void createDatabase(Connection c) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement(
                    "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                    .execute();
            c.prepareStatement("insert into person(name,score) values('FRED',21)").execute();
            c.prepareStatement("insert into person(name,score) values('JOSEPH',34)").execute();
            c.prepareStatement("insert into person(name,score) values('MARMADUKE',25)").execute();

            c.prepareStatement("create table person_clob (name varchar(50) not null,  document clob not null)")
                    .execute();
            c.prepareStatement("create table person_blob (name varchar(50) not null, document blob not null)")
                    .execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
