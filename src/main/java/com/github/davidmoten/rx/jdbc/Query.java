package com.github.davidmoten.rx.jdbc;

import rx.Observable;

/**
 * A database DML query, either update/insert or select.
 * 
 * @param <T>
 */
public interface Query<T> {

	/**
	 * Returns the sql statement for this query following JDBC format (? for
	 * parameters for instance).
	 * 
	 * @return jdbc sql
	 */
	String sql();

	/**
	 * Returns the parameters for the query in order of appearance as ? markers
	 * in the sql.
	 * 
	 * @return
	 */
	Observable<Object> parameters();

	/**
	 * Returns the Observables that have to complete before this query is
	 * started.
	 * 
	 * @return
	 */
	Observable<?> depends();

	/**
	 * Returns the query context including ConnectionProvider and thread pool.
	 * 
	 * @return
	 */
	QueryContext context();

}
