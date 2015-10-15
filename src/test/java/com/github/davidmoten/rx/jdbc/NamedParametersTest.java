package com.github.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
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
        assertParseUnchanged("select a, b from tbl");
    }

    @Test
    public void testNamedParametersAllMissingParametersShouldDoNothing() {
        DatabaseCreator.db().select("select name from person where name = :name").count()
                .subscribe();
    }

    @Test
    public void testDoubleColonNotModified() {
        JdbcQuery r = NamedParameters.parse("select a::varchar, b from tbl where a.name=:name");
        assertEquals("select a::varchar, b from tbl where a.name=?", r.sql());
        assertEquals(Arrays.asList("name"), r.names());
    }

    @Test
    public void testTripleColonNotModified() {
        JdbcQuery r = NamedParameters.parse("select a:::varchar, b from tbl where a.name=:name");
        assertEquals("select a:::varchar, b from tbl where a.name=?", r.sql());
        assertEquals(Arrays.asList("name"), r.names());
    }

    @Test
    public void testTerminatingColonNotModified() {
        assertParseUnchanged("select a:");
    }

    @Test
    public void testParseColonFollowedByNonIdentifierCharacter() {
        assertParseUnchanged("select a:||c from blah");
    }

    @Test
    public void testIsFollowedOrPrefixedByColon() {
        assertTrue(NamedParameters.isFollowedOrPrefixedByColon("a:bc", 0));
        assertFalse(NamedParameters.isFollowedOrPrefixedByColon("a:bc", 1));
        assertTrue(NamedParameters.isFollowedOrPrefixedByColon("a:bc", 2));
        assertFalse(NamedParameters.isFollowedOrPrefixedByColon(":bc", 0));
        assertTrue(NamedParameters.isFollowedOrPrefixedByColon(":bc", 1));
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testIsFollowedOrPrefixedByColonAtEndThrowsException() {
        NamedParameters.isFollowedOrPrefixedByColon("a:b", 2);
    }

    @Test
    public void testDoubleQuote() {
        assertParseUnchanged("select \":b\" from blah");
    }

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(NamedParameters.class);
    }

    private static void assertParseUnchanged(String sql) {
        JdbcQuery r = NamedParameters.parse(sql);
        assertEquals(sql, r.sql());
        assertTrue(r.names().isEmpty());
    }

}
