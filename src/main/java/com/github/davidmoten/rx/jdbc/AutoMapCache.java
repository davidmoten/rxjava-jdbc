package com.github.davidmoten.rx.jdbc;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.github.davidmoten.rx.jdbc.Util.Col;
import com.github.davidmoten.rx.jdbc.Util.IndexedCol;
import com.github.davidmoten.rx.jdbc.Util.NamedCol;
import com.github.davidmoten.rx.jdbc.annotations.Column;
import com.github.davidmoten.rx.jdbc.annotations.Index;

class AutoMapCache {
    final Map<String, Col> methodCols;
    public Class<?> cls;

    AutoMapCache(Class<?> cls) {
        this.cls = cls;
        this.methodCols = getMethodCols(cls);
    }

    private static Map<String, Col> getMethodCols(Class<?> cls) {
        Map<String, Col> methodCols = new HashMap<String, Col>();
        for (Method method : cls.getMethods()) {
            String name = method.getName();
            Column column = method.getAnnotation(Column.class);
            if (column != null) {
                checkHasNoParameters(method);
                // TODO check method has a mappable return type
                String col = column.value();
                if (col.equals(Column.NOT_SPECIFIED))
                    col = Util.camelCaseToUnderscore(name);
                methodCols.put(name, new NamedCol(col, method.getReturnType()));
            } else {
                Index index = method.getAnnotation(Index.class);
                if (index != null) {
                    // TODO check method has a mappable return type
                    checkHasNoParameters(method);
                    methodCols.put(name, new IndexedCol(index.value(), method.getReturnType()));
                }
            }
        }
        return methodCols;
    }

    private static void checkHasNoParameters(Method method) {
        if (method.getParameterTypes().length > 0) {
            throw new RuntimeException("mapped interface method cannot have parameters");
        }
    }

}
