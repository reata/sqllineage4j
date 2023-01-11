package io.github.reata.sqllineage4j.common.entity;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

@AutoValue
abstract public class ColumnQualifierTuple {
    public static ColumnQualifierTuple create(String column, @Nullable String qualifier) {
        return new AutoValue_ColumnQualifierTuple(column, qualifier);
    }

    abstract public String column();

    @Nullable
    abstract public String qualifier();
}
