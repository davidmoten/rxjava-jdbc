package com.github.davidmoten.rx.jdbc;

import rx.Observable;
import rx.Scheduler;

/**
 * A database DML query, either update/insert or select.
 */
public interface Query {

	/**
	 * Returns the sql statement for this query following JDBC format (? for
	 * parameters for instance).
	 * 
	 * @return jdbc sql
	 */
	String sql();

	/**
	 * Returns the parameters for the query in order of appearance as ? markers
	 * in the sql. May emit more than the number of parameters in one run of the
	 * query in which case the query would be run multiple times.
	 * 
	 * @return
	 */
	Observable<Parameter> parameters();

	/**
	 * Returns the Observables that have to complete before this query is
	 * started.
	 * 
	 * @return
	 */
	Observable<?> depends();

	/**
	 * Returns the query context including {@link ConnectionProvider} and
	 * {@link Scheduler}.
	 * 
	 * @return
	 */
	QueryContext context();

}
