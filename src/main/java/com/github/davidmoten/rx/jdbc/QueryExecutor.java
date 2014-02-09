package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.TO_EMPTY_LIST;

import java.util.List;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;
import rx.util.functions.Functions;

/**
 * Creates an {@link Observable} or type T corresponding to the results of a
 * query.
 * 
 * @param <T>
 */
public class QueryExecutor<T> {

	private final Query<T> query;

	/**
	 * Constructor.
	 * 
	 * @param query
	 */
	public QueryExecutor(Query<T> query) {
		this.query = query;
	}

	/**
	 * Returns the results of running the query.
	 * 
	 * @return
	 */
	public Observable<T> execute() {
		return createObservable();
	}

	private Observable<T> createObservable() {
		if (query instanceof QueryUpdate) {
			return createObservableUpdate((QueryUpdate<T>) query);
		} else {
			return createObservableSelect((QuerySelect<T>) query);
		}
	}

	private Observable<Object> parametersAfterDependencies() {
		// force the subscription of the dependencies before the parameters are
		// emitted
		return Observable.concat(query.depends()
				.filter(Functions.alwaysFalse()), query.parameters());
	}

	private Observable<Integer> singleIntegerAfterDependencies() {
		return Observable.concat(
				query.depends().count().filter(Functions.alwaysFalse()),
				Observable.from(1));
	}

	private Observable<T> createObservableSelect(QuerySelect<T> query) {
		final int numParamsPerQuery = Util.parametersPerSetCount(query.sql());

		if (numParamsPerQuery > 0)
			return parametersAfterDependencies().buffer(numParamsPerQuery)
					.flatMap(doSelect(query));
		else
			// run the query once with an empty list of parameters
			return singleIntegerAfterDependencies().map(TO_EMPTY_LIST).flatMap(
					doSelect(query));
	}

	private Observable<T> createObservableUpdate(QueryUpdate<T> query) {
		final int numParamsPerQuery = Util.parametersPerSetCount(query.sql());
		if (numParamsPerQuery > 0)
			return parametersAfterDependencies().buffer(numParamsPerQuery)
					.flatMap(doUpdate(query));
		else
			return singleIntegerAfterDependencies().map(TO_EMPTY_LIST).flatMap(
					doUpdate(query));
	}

	private Func1<List<Object>, Observable<T>> doSelect(
			final QuerySelect<T> query) {
		return new Func1<List<Object>, Observable<T>>() {
			@Override
			public Observable<T> call(final List<Object> params) {
				return createObservable(query, params);
			}
		};
	}

	private Observable<T> createObservable(final QuerySelect<T> query,
			final List<Object> params) {
		return Observable.create(new OnSubscribeFunc<T>() {
			@Override
			public Subscription onSubscribe(Observer<? super T> o) {
				final QuerySelectRunnable<T> q = new QuerySelectRunnable<T>(
						query, params, o);
				query.context().executor().execute(q);
				return createSubscription(q);
			}
		});
	}

	private Func1<List<Object>, Observable<T>> doUpdate(
			final QueryUpdate<T> query) {
		return new Func1<List<Object>, Observable<T>>() {
			@Override
			public Observable<T> call(final List<Object> params) {
				return createObservable(query, params);
			}
		};
	}

	private Observable<T> createObservable(final QueryUpdate<T> query,
			final List<Object> params) {
		return Observable.create(new OnSubscribeFunc<T>() {
			@Override
			public Subscription onSubscribe(Observer<? super T> o) {
				final QueryUpdateRunnable<T> q = new QueryUpdateRunnable<T>(
						query, params, o);
				query.context().executor().execute(q);
				return createSubscription(q);
			}

		});
	}

	private Subscription createSubscription(final Cancellable q) {
		return new Subscription() {
			@Override
			public void unsubscribe() {
				q.cancel();
			}
		};
	}
}