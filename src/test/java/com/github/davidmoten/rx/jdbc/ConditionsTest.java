package com.github.davidmoten.rx.jdbc;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class ConditionsTest {

    @Test
    public void testCheckTrueDoesNotThrowException() {
        Conditions.checkTrue(true);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckTrueThrowsException() {
        Conditions.checkTrue(false);
    }

    @Test
    public void testCheckFalseDoesNotThrowException() {
        Conditions.checkFalse(false);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckFalseThrowsException() {
        Conditions.checkFalse(true);
    }

    @Test
    public void testCheckNotNullDoesNotThrowException() {
        Conditions.checkNotNull(new Object());
    }

    @Test(expected = RuntimeException.class)
    public void testCheckNotNullThrowsException() {
        Conditions.checkNotNull(null);
    }

    @Test
    public void obtainCoverageOfPrivateConstructor() {
        Asserts.assertIsUtilityClass(Conditions.class);
    }

}
