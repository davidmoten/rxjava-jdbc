package com.github.davidmoten.rx.jdbc.tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Variable length tuple backed by a List.
 * 
 * @param <T>
 */
public class TupleN<T> {

    private final List<T> list;

    /**
     * Constructor.
     * 
     * @param list
     */
    public TupleN(List<T> list) {
        this.list = list;
    }

    public List<T> values() {
        // defensive copy
        return new ArrayList<T>(list);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TupleN<?> other = (TupleN<?>) obj;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!list.equals(other.list))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TupleN [values=" + list + "]";
    }

}
