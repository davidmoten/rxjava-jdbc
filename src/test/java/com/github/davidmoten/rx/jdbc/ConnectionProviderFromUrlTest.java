package com.github.davidmoten.rx.jdbc;

import org.junit.Test;

public class ConnectionProviderFromUrlTest {

    @Test(expected = RuntimeException.class)
    public void test() {
        new ConnectionProviderFromUrl("").get();
    }

}
