package com.github.davidmoten.rx.jdbc.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.METHOD;

@Target({ METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    public static final String NOT_SPECIFIED = "*COLUMN_NOT_SPECIFIED*";

    String value() default NOT_SPECIFIED;
}