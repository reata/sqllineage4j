package io.github.reata.sqllineage4j.common.entity;

import com.google.auto.value.AutoValue;

@AutoValue
abstract public class EdgeTuple {
    public static EdgeTuple create(Object source, String label, Object target) {
        return new AutoValue_EdgeTuple(source, label, target);
    }

    abstract public Object source();

    abstract public String label();

    abstract public Object target();
}
