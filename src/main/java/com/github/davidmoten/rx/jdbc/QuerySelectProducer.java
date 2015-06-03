package com.github.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Producer;
import rx.Subscriber;

import com.github.davidmoten.rx.RxUtil;

class QuerySelectProducer<T> implements Producer {

    private static final Logger log = LoggerFactory.getLogger(QuerySelectProducer.class);

    private final ResultSetMapper<? extends T> function;
    private final Subscriber<? super T> subscriber;
    private final Connection con;
    private final PreparedStatement ps;
    private final ResultSet rs;
    private volatile boolean keepGoing = true;

    private final AtomicLong requested = new AtomicLong(0);

    QuerySelectProducer(ResultSetMapper<? extends T> function, Subscriber<? super T> subscriber,
            Connection con, PreparedStatement ps, ResultSet rs) {
        this.function = function;
        this.subscriber = subscriber;
        this.con = con;
        this.ps = ps;
        this.rs = rs;
    }

    @Override
    public void request(long n) {
        if (requested.get() == Long.MAX_VALUE)
            // already started with fast path
            return;
        else if (n == Long.MAX_VALUE && requested.compareAndSet(0, Long.MAX_VALUE)) {
            requestAll();
        } else if (n > 0) {
            requestSome(n);
        }
    }

    private void requestAll() {
        // fast path
        try {
            while (keepGoing) {
                processRow(subscriber);
            }
            closeQuietly();
            complete(subscriber);
        } catch (Exception e) {
            closeAndHandleException(e);
        }
    }

    private void requestSome(long n) {
        // back pressure path
        // this algorithm copied generally from OnSubscribeFromIterable.java
        long previousCount = RxUtil.getAndAddRequest(requested, n);
        if (previousCount == 0) {
            try {
                while (true) {
                    long r = requested.get();
                    long numToEmit = r;

                    while (keepGoing && --numToEmit >= 0) {
                        processRow(subscriber);
                    }
                    if (keepGoing) {
                        if (requested.addAndGet(-r) == 0) {
                            return;
                        }
                    } else {
                        closeQuietly();
                        complete(subscriber);
                        return;
                    }
                }
            } catch (Exception e) {
                closeAndHandleException(e);
            }
        }
    }

    private void closeAndHandleException(Exception e) {
        try {
            closeQuietly();
        } finally {
            handleException(e, subscriber);
        }
    }

    /**
     * Processes each row of the {@link ResultSet}.
     * 
     * @param subscriber
     * 
     * @throws SQLException
     */
    private void processRow(Subscriber<? super T> subscriber) throws SQLException {
        checkSubscription(subscriber);
        if (!keepGoing)
            return;
        if (rs.next()) {
            log.trace("onNext");
            subscriber.onNext(function.call(rs));
        } else
            keepGoing = false;
    }

    /**
     * Tells observer that stream is complete and closes resources.
     * 
     * @param subscriber
     */
    private void complete(Subscriber<? super T> subscriber) {
        if (subscriber.isUnsubscribed()) {
            log.debug("unsubscribed");
        } else {
            log.debug("onCompleted");
            subscriber.onCompleted();
        }
    }

    /**
     * Tells observer about exception.
     * 
     * @param e
     * @param subscriber
     */
    private void handleException(Exception e, Subscriber<? super T> subscriber) {
        log.debug("onError: " + e.getMessage());
        if (subscriber.isUnsubscribed())
            log.debug("unsubscribed");
        else {
            subscriber.onError(e);
        }
    }

    /**
     * Closes connection resources (connection, prepared statement and result
     * set).
     */
    private void closeQuietly() {
        log.debug("closing rs");
        Util.closeQuietly(rs);
        log.debug("closing ps");
        Util.closeQuietly(ps);
        log.debug("closing con");
        Util.closeQuietlyIfAutoCommit(con);
        log.debug("closed");
    }

    /**
     * If subscribe unsubscribed sets keepGoing to false.
     * 
     * @param subscriber
     */
    private void checkSubscription(Subscriber<? super T> subscriber) {
        if (subscriber.isUnsubscribed()) {
            keepGoing = false;
            log.debug("unsubscribing");
        }
    }

}
