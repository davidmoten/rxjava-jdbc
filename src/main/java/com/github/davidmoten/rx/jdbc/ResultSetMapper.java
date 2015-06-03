package com.github.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetMapper<T> {

    T call(ResultSet rs) throws SQLException;
}
