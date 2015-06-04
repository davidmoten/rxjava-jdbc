package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.DatabaseCreator.connectionProvider;
import static com.github.davidmoten.rx.jdbc.DatabaseCreator.createDatabase;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public class ConnectionDemoTest {

    @Test
    public void testOldStyle() {
        Connection con = connectionProvider().get();
        createDatabase(con);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("select name from person where name > ?");
            ps.setObject(1, "ALEX");
            rs = ps.executeQuery();
            List<String> list = new ArrayList<String>();
            while (rs.next()) {
                list.add(rs.getString(1));
            }
            assertEquals(asList("FRED", "JOSEPH", "MARMADUKE"), list);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            try {
                con.close();
            } catch (SQLException e) {
            }
        }

    }

}
