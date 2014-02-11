package com.github.davidmoten.rx.jdbc;

import rx.util.functions.Func1;

/**
 * Encapsulate a query parameter.
 */
class Parameter {

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
