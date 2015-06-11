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

@State(Scope.Benchmark)
public class Benchmarks {

    public final Connection con = new ConnectionNonClosing(DatabaseCreator.nextConnection());
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

}
