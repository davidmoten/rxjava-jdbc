package com.github.davidmoten.rx.jdbc;

import static com.github.davidmoten.rx.jdbc.Util.TO_EMPTY_PARAMETER_LIST;
import static com.github.davidmoten.rx.jdbc.Util.concatButIgnoreFirstSequence;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;
import rx.util.functions.Func1;

/**
 * Creates an {@link Observable} of the results of running either an update or
 * select query.
 */
public class QueryExecutor {

	/**
	 * The query to be executed.
	 */
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
	 * Returns the results of running a select query.
	 * 
	 * @return
	 */
	public Observable<ResultSet> execute() {
		return createObservable(numParamsPerQuery(),
				doSelect((QuerySelect) query));
	}

	/**
	 * Returns the results of an update query. Should be an {@link Observable}
	 * of size 1 containing the number of records affected by the update (or
	 * insert) statement.
	 * 
	 * @param query
	 * @return
	 */
	public Observable<Integer> executeUpdate() {
		return createObservable(numParamsPerQuery(),
				doUpdate((QueryUpdate) query));
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
	 * Returns the number of parameters required to run this query once. Roughly
	 * corresponds to the number of ? characters in the sql but have to watch
	 * out for ? characters within quoted strings.
	 * 
	 * @return number of parameters in query sql
	 */
	private int numParamsPerQuery() {
		return Util.parametersPerSetCount(query.sql());
	}

	private <T> Observable<T> createObservable(final int numParamsPerQuery,
			Func1<List<Parameter>, Observable<T>> function) {
		if (numParamsPerQuery > 0)
			return parametersAfterDependencies().buffer(numParamsPerQuery)
					.flatMap(function);
		else {
			return singleIntegerAfterDependencies()
					.map(TO_EMPTY_PARAMETER_LIST).flatMap(function);
		}
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
	 *            parameters prefixed by dependencies that do not emit.
	 * @return
	 */
	private Observable<ResultSet> createObservable(final QuerySelect query,
			final List<Parameter> params) {
		return Observable.create(new OnSubscribeFunc<ResultSet>() {
			@Override
			public Subscription onSubscribe(Observer<? super ResultSet> o) {
				final QuerySelectRunnable q = new QuerySelectRunnable(query,
						params, o);
				return subscribe(query, q);
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
	 * @param parameters
	 * @return
	 */
	private Observable<Integer> createObservable(final QueryUpdate query,
			final List<Parameter> parameters) {
		return Observable.create(new OnSubscribeFunc<Integer>() {
			@Override
			public Subscription onSubscribe(Observer<? super Integer> o) {
				final QueryUpdateRunnable q = new QueryUpdateRunnable(query,
						parameters, o);
				return subscribe(query, q);
			}
		});
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
	private static <T extends Runnable & Cancellable> Subscription subscribe(
			Query query, T runnable) {
		Subscription sub = Schedulers.executor(query.context().executor())
				.schedule(Util.toAction0(runnable));
		return Subscriptions.from(sub, createSubscription(runnable));
	}

	/**
	 * Create an rx {@link Subscription} that cancels the given
	 * {@link Cancellable} on unsubscribe.
	 * 
	 * @param cancellable
	 * @return
	 */
	private static Subscription createSubscription(final Cancellable cancellable) {
		return new Subscription() {
			@Override
			public void unsubscribe() {
				cancellable.cancel();
			}
		};
	}
}