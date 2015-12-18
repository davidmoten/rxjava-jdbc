package com.github.davidmoten.rx.jdbc;

final class Batch {

    private static final ThreadLocal<Batch> batch = new ThreadLocal<Batch>() {
        @Override
        protected Batch initialValue() {
            return new Batch(1, 0);
        }
    };

    public static Batch get() {
        return batch.get();
    }

    public static void set(Batch b) {
        batch.set(b);
    }

    final int size;
    int added;
    private PreparedStatementBatch ps;

    Batch(int size, int added) {
        this.size = size;
        this.added = added;
    }

    Batch(int size) {
        this(size, 0);
    }

    Batch addOne() {
        added++;
        return this;
    }

    boolean complete() {
        return size == added;
    }

    boolean enabled() {
        return size > 1;
    }

    public Batch reset() {
        added = 0;
        return this;
    }

    public int countAdded() {
        return added;
    }

    public Batch setPreparedStatement(PreparedStatementBatch ps) {
        this.ps = ps;
        return this;
    }

    public PreparedStatementBatch getPreparedStatement() {
        return this.ps;
    }

}
