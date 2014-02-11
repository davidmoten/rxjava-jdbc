package com.github.davidmoten.rx.jdbc;

import rx.util.functions.Func1;

class Parameter {

	private final Object parameter;

	Parameter(Object parameter) {
		super();
		this.parameter = parameter;
	}

	Object getParameter() {
		return parameter;
	}

	static final Func1<Object, Parameter> TO_PARAMETER = new Func1<Object, Parameter>() {

		@Override
		public Parameter call(Object parameter) {
			return new Parameter(parameter);
		}
	};

}
