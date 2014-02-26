package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.DatabaseCreator.connectionProvider;

import org.junit.Test;

public class HandlersTest {

	@Test(expected = RuntimeException.class)
	public void testLogOnErrorHandler() {
		Database db = Database.builder()
				.connectionProvider(connectionProvider())
				.handler(Handlers.LOG_ON_ERROR_HANDLER).build();
		db.select("select name from perzon").getAs(String.class).first()
				.toBlockingObservable().single();
	}
}
