package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.github.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

class ResultSetCache {

    final ResultSet rs;
    final Map<String, Integer> colIndexes;

    ResultSetCache(ResultSet rs) {
        this.rs = rs;

        this.colIndexes = collectColIndexes(rs);
    }

    private static Map<String, Integer> collectColIndexes(ResultSet rs) {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        try {
            ResultSetMetaData metadata = rs.getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                map.put(metadata.getColumnName(i).toUpperCase(), i);
            }
            return map;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
