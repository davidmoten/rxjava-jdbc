package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

public class DatabaseDerbyTest {

    /**
     * Black hole for output used instead of derby.log.
     */
    public static final OutputStream DEV_NULL = new OutputStream() {
        public void write(int b) {
            // do nothing
        }
    };

    @Test
    @Ignore
    public void testReturnGeneratedKeys() throws SQLException {
        setup();
        Connection con = DriverManager.getConnection("jdbc:derby:memory:derby1;create=true");
        con.prepareStatement(
                "create table note(id integer not null generated always as identity (start with 1, increment by 1),\n"
                        + "text varchar(255) not null)")
                .execute();
        Database db = Database.from(con);
        assertEquals(Arrays.asList(1, 2),
                db.update("insert into note(text) values(?),(?)").parameters("boo", "to")
                        .returnGeneratedKeys().getAs(Integer.class).toList().toBlocking().single());
    }

    private void setup() {
        // suppress creation of derby.log in project root
        System.setProperty("derby.stream.error.field",
                DatabaseDerbyTest.class.getName() + ".DEV_NULL");
    }
}
