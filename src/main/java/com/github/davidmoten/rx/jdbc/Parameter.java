package com.github.davidmoten.rx.jdbc;

import rx.functions.Func1;

/**
 * Encapsulates a query parameter.
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
    
    

    @Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Parameter [parameter=");
		builder.append(parameter);
		builder.append("]");
		return builder.toString();
	}

	/**
     * A conversion function for use in Observable.map().
     */
    static final Func1<Object, Parameter> TO_PARAMETER = new Func1<Object, Parameter>() {

        @Override
        public Parameter call(Object parameter) {
            Conditions.checkFalse(parameter instanceof Parameter);
            return new Parameter(parameter);
        }
    };

}
