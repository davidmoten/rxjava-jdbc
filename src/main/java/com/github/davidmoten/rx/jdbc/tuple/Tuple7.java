package com.github.davidmoten.rx.jdbc.tuple;

/**
 * An explicitly typed tuple. *
 * 
 * @param <T1>
 * @param <T2>
 * @param <T3>
 * @param <T4>
 * @param <T5>
 * @param <T6>
 * @param <T7>
 */
public class Tuple7<T1, T2, T3, T4, T5, T6, T7> {

    private final T1 value1;
    private final T2 value2;
    private final T3 value3;
    private final T4 value4;
    private final T5 value5;
    private final T6 value6;
    private final T7 value7;

    /**
     * Constructor.
     * 
     * @param value1
     * @param value2
     * @param value3
     * @param value4
     * @param value5
     * @param value6
     * @param value7
     */
    public Tuple7(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
        this.value4 = value4;
        this.value5 = value5;
        this.value6 = value6;
        this.value7 = value7;
    }

    public T1 value1() {
        return value1;
    }

    public T2 value2() {
        return value2;
    }

    public T3 value3() {
        return value3;
    }

    public T4 value4() {
        return value4;
    }

    public T5 value5() {
        return value5;
    }

    public T6 value6() {
        return value6;
    }

    public T7 value7() {
        return value7;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value1 == null) ? 0 : value1.hashCode());
        result = prime * result + ((value2 == null) ? 0 : value2.hashCode());
        result = prime * result + ((value3 == null) ? 0 : value3.hashCode());
        result = prime * result + ((value4 == null) ? 0 : value4.hashCode());
        result = prime * result + ((value5 == null) ? 0 : value5.hashCode());
        result = prime * result + ((value6 == null) ? 0 : value6.hashCode());
        result = prime * result + ((value7 == null) ? 0 : value7.hashCode());
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
        Tuple7<?, ?, ?, ?, ?, ?, ?> other = (Tuple7<?, ?, ?, ?, ?, ?, ?>) obj;
        if (value1 == null) {
            if (other.value1 != null)
                return false;
        } else if (!value1.equals(other.value1))
            return false;
        if (value2 == null) {
            if (other.value2 != null)
                return false;
        } else if (!value2.equals(other.value2))
            return false;
        if (value3 == null) {
            if (other.value3 != null)
                return false;
        } else if (!value3.equals(other.value3))
            return false;
        if (value4 == null) {
            if (other.value4 != null)
                return false;
        } else if (!value4.equals(other.value4))
            return false;
        if (value5 == null) {
            if (other.value5 != null)
                return false;
        } else if (!value5.equals(other.value5))
            return false;
        if (value6 == null) {
            if (other.value6 != null)
                return false;
        } else if (!value6.equals(other.value6))
            return false;
        if (value7 == null) {
            if (other.value7 != null)
                return false;
        } else if (!value7.equals(other.value7))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Tuple7 [value1=");
        builder.append(value1);
        builder.append(", value2=");
        builder.append(value2);
        builder.append(", value3=");
        builder.append(value3);
        builder.append(", value4=");
        builder.append(value4);
        builder.append(", value5=");
        builder.append(value5);
        builder.append(", value6=");
        builder.append(value6);
        builder.append(", value7=");
        builder.append(value7);
        builder.append("]");
        return builder.toString();
    }

}
