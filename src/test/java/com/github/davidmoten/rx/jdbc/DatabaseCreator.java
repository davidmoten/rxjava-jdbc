package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

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
            throw new SQLRuntimeException(e);
        }
        return new Database(cp);
    }

    public static Database createDatabase(DataSource dataSource) {
        return createDatabase(new ConnectionProviderFromDataSource(dataSource));
    }

    public static Database createDatabase(ConnectionProvider cp) {
        Database db = Database.from(cp);
        Connection con = cp.get();
        createDatabase(con);
        try {
            con.close();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return db;
    }

    static Connection nextConnection() {
        try {
            Connection con = DriverManager.getConnection(DatabaseCreator.nextUrl());
            DatabaseCreator.createDatabase(con);
            return con;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
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

            c.prepareStatement(
                    "create table person_clob (name varchar(50) not null,  document clob)")
                    .execute();
            c.prepareStatement(
                    "create table person_blob (name varchar(50) not null, document blob)")
                    .execute();
            c.prepareStatement(
                    "create table address (address_id int primary key, full_address varchar(255) not null)")
                    .execute();
            c.prepareStatement(
                    "insert into address(address_id, full_address) values(1,'57 Something St, El Barrio, Big Place')")
                    .execute();
            c.prepareStatement(
                    "create table note(id bigint auto_increment primary key, text varchar(255))")
                    .execute();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
