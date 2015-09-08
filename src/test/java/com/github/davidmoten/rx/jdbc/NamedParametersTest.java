package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.github.davidmoten.rx.jdbc.NamedParameters.JdbcQuery;

public class NamedParametersTest {

    @Test
    public void testSelect() {
        JdbcQuery r = NamedParameters.parse(
                "select a, b from tbl where a.name=:name and b.name=:name and c.description = :description");
        assertEquals("select a, b from tbl where a.name=? and b.name=? and c.description = ?",
                r.sql());
        assertEquals(Arrays.asList("name", "name", "description"), r.names());
    }

    @Test
    public void testSelectWithOneNamedParameterAndColonNameInQuotes() {
        JdbcQuery r = NamedParameters
                .parse("select a, b from tbl where a.name=:name and b.name=':name'");
        assertEquals("select a, b from tbl where a.name=? and b.name=':name'", r.sql());
        assertEquals(Arrays.asList("name"), r.names());
    }

    @Test
    public void testSelectWithNoNamedParameters() {
        JdbcQuery r = NamedParameters.parse("select a, b from tbl");
        assertEquals("select a, b from tbl", r.sql());
        assertTrue(r.names().isEmpty());
    }

    @Test
    public void testNamedParametersAllMissingParametersShouldDoNothing() {
        DatabaseCreator.db().select("select name from person where name = :name").count()
                .subscribe();
    }
}
