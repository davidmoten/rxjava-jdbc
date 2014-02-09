package com.github.davidmoten.rx.jdbc;

/**
 * A class with this interface has an action perhaps long running that should be
 * interrupted via a cancel() method.
 * 
 */
interface Cancellable {
	/**
	 * Cancels the action undertaken by this. May be asynchronous or synchronous
	 * cancellation and there is no guarantee of effect.
	 */
	void cancel();
}
