package com.github.davidmoten.rx;

import static java.lang.annotation.ElementType.METHOD;
import static org.junit.Assert.assertEquals;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import org.junit.Test;

public class DynamicProxyTest {

    @Target({ METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface Column {
        String value();
    }

    @Target({ METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface Index {
        /**
         * 1 based index corresponding the index in a
         * <code>ResultSet.getObject(index)</code> call.
         * 
         * @return the 1 based index that the annotated method corresponds to in
         *         the ResultSet
         */
        int value();
    }

    public static interface Thing {
        @Column("table_id")
        int id();

        @Index(2)
        String name();

        @Column("desc")
        String description();
    }

    @Test
    public void testDynamicProxy() {
        Thing t = ProxyService.newInstance(Thing.class);
        assertEquals(123, t.id());
        assertEquals("fred name", t.name());
        assertEquals("he's the business! desc", t.description());
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
            if (a == null)
                column = null;
            else
                column = a.value();
            String name = m.getName();
            if (name.equals("id")) {
                return 123;
            } else if (name.equals("name")) {
                return "fred " + column;
            } else if (name.equals("description")) {
                return "he's the business! " + column;
            } else
                throw new RuntimeException("unexpected");
        }
    }

}
