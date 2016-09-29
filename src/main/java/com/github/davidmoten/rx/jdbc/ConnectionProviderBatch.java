package com.github.davidmoten.rx.jdbc;

class ConnectionProviderBatch implements ConnectionProvider {

	private final int batchSize;
	private final ConnectionProvider cp;
	private ConnectionBatch con;

	ConnectionProviderBatch(ConnectionProvider cp, int batchSize) {
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
