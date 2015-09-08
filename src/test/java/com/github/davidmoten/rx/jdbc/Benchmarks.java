package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import rx.Observable;
import rx.functions.Func1;

@State(Scope.Benchmark)
public class Benchmarks {

    public Connection con = createConnection();
    public final Database db = Database.from(con);

    @Benchmark
    public void selectUsingLibraryUsingExplicitMapping() {
        db.select("select name from person")
                //
                .get(new ResultSetMapper<String>() {
                    @Override
                    public String call(ResultSet rs) throws SQLException {
                        return rs.getString(1);
                    }
                })
                //
                .toList()
                // go
                .toBlocking().single();
    }

    @Benchmark
    public void selectUsingRawJdbc() {
        try (PreparedStatement ps = con.prepareStatement("select name from person");) {
            List<String> list = new ArrayList<String>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection createConnection() {
        Connection con = new ConnectionNonClosing(DatabaseCreator.nextConnection());
        Database db = Database.from(con);
        // insert another 1000 people
        db.update("insert into person(name, score) values(?,?)").parameters(
                Observable.range(1, 1000).concatMap(new Func1<Integer, Observable<Object>>() {
                    @Override
                    public Observable<Object> call(Integer n) {
                        return Observable.<Object> just("person" + n, n);
                    }
                }));
        return con;
    }

}
