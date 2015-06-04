package com.github.davidmoten.rx.jdbc.exceptions;

import java.sql.SQLException;

public class SQLRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -2895807523709102758L;

    public SQLRuntimeException(SQLException e) {
       super(e);
    }
}
