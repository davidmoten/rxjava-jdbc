package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.TO_EMPTY_PARAMETER_LIST;
import static com.github.davidmoten.rx.jdbc.Util.concatButIgnoreFirstSequence;

import java.sql.ResultSet;
import java.util.List;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

/**
 * Creates an {@link Observable} of the results of running either an update or
 * select query.
 */
public class QueryExecutor {

	private final Query query;

	/**
	 * Constructor.
	 * 
	 * @param query
	 */
	public QueryExecutor(Query query) {
		this.query = query;
	}

	/**
	 * Returns the results of running the query.
	 * 
	 * @return
	 */
	public Observable<ResultSet> execute() {
		return createObservableSelect((QuerySelect) query);
	}

	public Observable<Integer> executeUpdate() {
		return createObservableUpdate((QueryUpdate) query);
	}

	/**
	 * Returns query.getParameters() {@link Observable} but only after query
	 * dependencies have been fully emitted (and ignored).
	 * 
	 * @return query parameters
	 */
	private Observable<Parameter> parametersAfterDependencies() {
		return concatButIgnoreFirstSequence(query.depends(), query.parameters());
	}

	/**
	 * Returns {@link Observable} with one item 1 but only after query
	 * dependencies have been fully emitted (and ignored).
	 * 
	 * @return {@link Observable} with one element 1
	 */
	private Observable<Integer> singleIntegerAfterDependencies() {
		return concatButIgnoreFirstSequence(query.depends(), Observable.from(1));
	}

	/**
	 * Returns the {@link ResultSet}s for each iteration of the result of the
	 * query.
	 * 
	 * @param query
	 * @return
	 */
	private Observable<ResultSet> createObservableSelect(QuerySelect query) {
		final int numParamsPerQuery = Util.parametersPerSetCount(query.sql());

		if (numParamsPerQuery > 0)
			return parametersAfterDependencies().buffer(numParamsPerQuery)
					.flatMap(doSelect(query));
		else
			// run the query once with an empty list of parameters
			return singleIntegerAfterDependencies()
					.map(TO_EMPTY_PARAMETER_LIST).flatMap(doSelect(query));
	}

	/**
	 * Returns the results of an update query. Should be an {@link Observable}
	 * of size 1 containing the number of records affected by the update (or
	 * insert) statement.
	 * 
	 * @param query
	 * @return
	 */
	private Observable<Integer> createObservableUpdate(QueryUpdate query) {
		final int numParamsPerQuery = Util.parametersPerSetCount(query.sql());
		if (numParamsPerQuery > 0)
			return parametersAfterDependencies().buffer(numParamsPerQuery)
					.flatMap(doUpdate(query));
		else
			return singleIntegerAfterDependencies()
					.map(TO_EMPTY_PARAMETER_LIST).flatMap(doUpdate(query));
	}

	/**
	 * Returns a {@link Func1} that itself returns the results of pushing
	 * parameters through a select query.
	 * 
	 * @param query
	 * @return
	 */
	private Func1<List<Parameter>, Observable<ResultSet>> doSelect(
			final QuerySelect query) {
		return new Func1<List<Parameter>, Observable<ResultSet>>() {
			@Override
			public Observable<ResultSet> call(final List<Parameter> params) {
				return createObservable(query, params);
			}
		};
	}

	/**
	 * Returns the results of an update query. Should be an {@link Observable}
	 * of size 1 containing the number of records affected by the update (or
	 * insert) statement.
	 * 
	 * @param query
	 * @param params
	 * @return
	 */
	private Observable<ResultSet> createObservable(final QuerySelect query,
			final List<Parameter> params) {
		return Observable.create(new OnSubscribeFunc<ResultSet>() {
			@Override
			public Subscription onSubscribe(Observer<? super ResultSet> o) {
				final QuerySelectRunnable q = new QuerySelectRunnable(query,
						params, o);
				query.context().executor().execute(q);
				return createSubscription(q);
			}
		});
	}

	/**
	 * Returns a {@link Func1} that itself returns the results of pushing
	 * parameters through an update query.
	 * 
	 * @param query
	 * @return
	 */
	private Func1<List<Parameter>, Observable<Integer>> doUpdate(
			final QueryUpdate query) {
		return new Func1<List<Parameter>, Observable<Integer>>() {
			@Override
			public Observable<Integer> call(final List<Parameter> params) {
				return createObservable(query, params);
			}
		};
	}

	/**
	 * Returns the results of an update query. Should return an
	 * {@link Observable} of size one containing the rows affected count.
	 * 
	 * @param query
	 * @param params
	 * @return
	 */
	private Observable<Integer> createObservable(final QueryUpdate query,
			final List<Parameter> params) {
		return Observable.create(new OnSubscribeFunc<Integer>() {
			@Override
			public Subscription onSubscribe(Observer<? super Integer> o) {
				final QueryUpdateRunnable q = new QueryUpdateRunnable(query,
						params, o);
				query.context().executor().execute(q);
				return createSubscription(q);
			}

		});
	}

	/**
	 * Create an rx {@link Subscription} that cancels the given
	 * {@link Cancellable} on unsubscribe.
	 * 
	 * @param cancellable
	 * @return
	 */
	private Subscription createSubscription(final Cancellable cancellable) {
		return new Subscription() {
			@Override
			public void unsubscribe() {
				cancellable.cancel();
			}
		};
	}
}