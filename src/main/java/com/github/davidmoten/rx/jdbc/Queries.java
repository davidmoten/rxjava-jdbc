package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.TO_EMPTY_PARAMETER_LIST;
import static com.github.davidmoten.rx.jdbc.Util.concatButIgnoreFirstSequence;

import java.util.List;
import java.util.concurrent.ExecutorService;

import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;

/**
 * Utility methods for queries.
 */
final class Queries {
	/**
	 * Returns the number of parameters required to run this query once. Roughly
	 * corresponds to the number of ? characters in the sql but have to watch
	 * out for ? characters within quoted strings.
	 * 
	 * @return number of parameters in query sql
	 */
	static int numParamsPerQuery(Query query) {
		return Util.parametersPerSetCount(query.sql());
	}

	/**
	 * Returns query.getParameters() {@link Observable} but only after query
	 * dependencies have been fully emitted (and ignored).
	 * 
	 * @return query parameters
	 */
	static Observable<Parameter> parametersAfterDependencies(Query query) {
		return concatButIgnoreFirstSequence(query.depends(), query.parameters());
	}

	/**
	 * Returns {@link Observable} with one item 1 but only after query
	 * dependencies have been fully emitted (and ignored).
	 * 
	 * @param query
	 * 
	 * @return {@link Observable} with one element 1
	 */
	static Observable<Integer> singleIntegerAfterDependencies(Query query) {
		return concatButIgnoreFirstSequence(query.depends(), Observable.from(1));
	}

	/**
	 * If the number of parameters in a query is >0 then group the parameters in
	 * lists of that number in size but only after the dependencies have been
	 * completed. If the number of parameteres is zero then return an observable
	 * containing one item being an empty list.
	 * 
	 * @param query
	 * @return
	 */
	static Observable<List<Parameter>> bufferedParameters(Query query) {
		int numParamsPerQuery = numParamsPerQuery(query);
		if (numParamsPerQuery > 0)
			return parametersAfterDependencies(query).buffer(numParamsPerQuery);
		else
			return singleIntegerAfterDependencies(query).map(
					TO_EMPTY_PARAMETER_LIST);
	}

	/**
	 * Schedules the runnable for execution using the query context
	 * {@link ExecutorService} and return a subscription allowing for cancelling
	 * of the scheduling as well as of a running action.
	 * 
	 * @param query
	 * @param runnable
	 * @return
	 */
	static <T extends Runnable & Cancellable> Subscription schedule(
			Query query, T runnable) {
		Subscription sub = Schedulers.executor(query.context().executor())
				.schedule(Util.toAction0(runnable));
		return Subscriptions.from(sub, Util.createSubscription(runnable));
	}
}
