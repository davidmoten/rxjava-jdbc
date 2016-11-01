package com.github.davidmoten.rx.jdbc;

import com.github.davidmoten.guavamini.Preconditions;

class ConnectionProviderBatch implements ConnectionProvider {

    private final int batchSize;
    private final ConnectionProvider cp;
    private ConnectionBatch con;

    ConnectionProviderBatch(ConnectionProvider cp, int batchSize) {
        Preconditions.checkNotNull(cp, "cp should not be null");
        this.cp = cp;
        this.batchSize = batchSize;
    }

    @Override
    public ConnectionBatch get() {
        if (con == null) {
            con = new ConnectionBatch(cp.get(), batchSize);
        }
        return con;
    }

    @Override
    public void close() {
        cp.close();
    }

}
