package com.github.davidmoten.rx.jdbc;

import java.sql.PreparedStatement;

final class Batch {

    private static final ThreadLocal<Batch> batch = new ThreadLocal<Batch>() {
        @Override
        protected Batch initialValue() {
            return Batch.NO_BATCHING;
        }
    };

    public static Batch get() {
        return batch.get();
    }

    public static void set(Batch b) {
        batch.set(b);
    }

    final int size;
    final int added;
    private PreparedStatement ps;

    Batch(int size, int added) {
        this.size = size;
        this.added = added;
    }

    Batch(int size) {
        this(size, 0);
    }

    Batch addOne() {
        return new Batch(size, added + 1);
    }

    boolean complete() {
        return size == added;
    }

    boolean enabled() {
        return size == 1;
    }

    static final Batch NO_BATCHING = new Batch(1, 0);

    public Batch reset() {
        return new Batch(size);
    }

    public int countAdded() {
        return added;
    }

    public Batch setPreparedStatement(PreparedStatementBatch ps) {
        this.ps = ps;
        return this;
    }

    public PreparedStatement getPreparedStatement() {
        return this.ps;
    }

}
