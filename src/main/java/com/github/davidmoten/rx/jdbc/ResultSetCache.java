package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.Map;

import com.github.davidmoten.rx.jdbc.Util.Col;

class ResultSetCache {

    final ResultSet rs;
    final Map<String, Integer> colIndexes;
    final Map<String, Col> methodCols;

    ResultSetCache(ResultSet rs, Map<String, Integer> colIndexes, Map<String, Col> methodCols) {
        this.rs = rs;
        this.colIndexes = colIndexes;
        this.methodCols = methodCols;
    }

}
