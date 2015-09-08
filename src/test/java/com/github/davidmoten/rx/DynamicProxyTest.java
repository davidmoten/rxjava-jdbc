package com.github.davidmoten.rx;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;

import org.junit.Test;

import com.github.davidmoten.rx.jdbc.annotations.Column;
import com.github.davidmoten.rx.jdbc.annotations.Index;

public class DynamicProxyTest {

    public static interface Thing {
        @Column("table_id")
        int id();

        int nonNullNumber();

        @Index(2)
        String name();

        @Column("desc")
        String description();
    }

    @Test
    public void testDynamicProxy() {
        Thing t = ProxyService.newInstance(Thing.class);
        assertEquals(123, t.id());
        assertEquals("fred 2", t.name());
        assertEquals("he's the business! desc", t.description());
    }

    @Test(expected = NullPointerException.class)
    public void testDynamicProxyReturnsNullForNonNullMethod() {
        Thing t = ProxyService.newInstance(Thing.class);
        t.nonNullNumber();
    }

    public static class ProxyService implements java.lang.reflect.InvocationHandler {

        @SuppressWarnings("unchecked")
        public static <T> T newInstance(Class<T> cls) {
            return (T) java.lang.reflect.Proxy.newProxyInstance(cls.getClassLoader(),
                    new Class[] { cls }, new ProxyService());
        }

        public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
            Column a = m.getAnnotation(Column.class);
            final String column;
            if (a == null || a.value().equals(Column.NOT_SPECIFIED))
                column = null;
            else
                column = a.value();
            String name = m.getName();
            if (name.equals("id")) {
                return 123;
            } else if (name.equals("name")) {
                return "fred " + m.getAnnotation(Index.class).value();
            } else if (name.equals("description")) {
                return "he's the business! " + column;
            } else if (name.equals("nonNullNumber"))
                return null;
            else
                throw new RuntimeException("unexpected");
        }

    }

}
