package com.github.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
final class ArrayListFinal<T> extends ArrayList<T>  {

	ArrayListFinal(List<T> list) {
		super(list);
	}

}
