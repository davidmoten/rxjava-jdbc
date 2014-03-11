package com.github.davidmoten.rx;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

public class RxUtil {

    /**
     * slf4j logger.
     */
    private static final Logger log = LoggerFactory.getLogger(RxUtil.class);

    /**
     * Returns the concatenation of two {@link Observable}s but the first
     * sequence will be emitted in its entirety and ignored before o2 starts
     * emitting.
     * 
     * @param o1
     *            the sequence to ignore
     * @param o2
     *            the sequence to emit after o1 ignored
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Observable<T> concatButIgnoreFirstSequence(Observable<?> o1, Observable<T> o2) {
        return Observable.concat((Observable<T>) o1.ignoreElements(), o2);
    }

    /**
     * Logs errors and onNext at info level using slf4j {@link Logger}.
     * 
     * @return
     */
    public static <T> Observer<? super T> log() {
        return new Observer<T>() {

            @Override
            public void onCompleted() {
                // do nothing
            }

            @Override
            public void onError(Throwable e) {
                log.error(e.getMessage(), e);
            }

            @Override
            public void onNext(T t) {
                log.info(t + "");
            }
        };
    }

    /**
     * Returns a constant value.
     * 
     * @param s
     * @return
     */
    public static <R, S> Func1<R, S> constant(final S s) {
        return new Func1<R, S>() {
            @Override
            public S call(R t1) {
                return s;
            }
        };
    }

    public static <R, T> Operator<R, T> toOperator(Func1<Observable<T>, Observable<R>> operation) {
        return OperatorFromOperation.toOperator(operation);
    }
    
    public static <T> UnsubscribeDetector<T> detectUnsubscribe() {
    	return UnsubscribeDetector.detect();
    }

	public static <T> CounterAction<T> counter() {
		return new CounterAction<T>();
	}
	
	public static class CounterAction<T> implements Action1<T> {
		private final AtomicLong count = new AtomicLong(0);
		
		public Observable<Long> count() {
			return Observable.create(new OnSubscribe<Long>() {

				@Override
				public void call(Subscriber<? super Long> subscriber) {
					subscriber.onNext(count.get());
					subscriber.onCompleted();
				}}); 
		}

		@Override
		public void call(T t) {
			count.incrementAndGet();
		}
	}
	
	public static <T extends Number> Func1<T,Boolean> greaterThanZero() {
		return new Func1<T,Boolean>() {

			@Override
			public Boolean call(T t) {
				return t.doubleValue()>0;
			}
		};
	}	
}
