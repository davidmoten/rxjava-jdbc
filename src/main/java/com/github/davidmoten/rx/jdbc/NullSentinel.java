package com.github.davidmoten.rx.jdbc;

public final class NullSentinel {
    private final int jdbcType;

    private NullSentinel(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public static NullSentinel create(int jdbcType) {
        return new NullSentinel(jdbcType);
    }
    
    public int getJdbcType() {
        return jdbcType;
    }

}
