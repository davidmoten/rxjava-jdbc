package com.github.davidmoten.rx.jdbc;

import rx.util.functions.Func1;

/**
 * Encapsulate a query parameter.
 */
final class Parameter {

	/**
	 * Actual query parameter value to be encapsulated.
	 */
	private final Object parameter;

	/**
	 * Constructor.
	 * 
	 * @param parameter
	 */
	Parameter(Object parameter) {
		super();
		this.parameter = parameter;
	}

	/**
	 * Returns the parameter value.
	 * 
	 * @return
	 */
	Object getValue() {
		return parameter;
	}

	/**
	 * A conversion function for use in Observable.map().
	 */
	static final Func1<Object, Parameter> TO_PARAMETER = new Func1<Object, Parameter>() {

		@Override
		public Parameter call(Object parameter) {
			Conditions.checkTrue(!(parameter instanceof Parameter));
			return new Parameter(parameter);
		}
	};

}
