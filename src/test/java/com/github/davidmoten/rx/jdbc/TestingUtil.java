package com.github.davidmoten.rx.jdbc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class TestingUtil {

	public static <T> void instantiateUsingPrivateConstructor(Class<T> cls) {
		try {
			Constructor<T> constructor = cls
					.getDeclaredConstructor(new Class[0]);
			constructor.setAccessible(true);
			constructor.newInstance(new Object[0]);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}
	}
}
