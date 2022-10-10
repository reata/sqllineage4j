package io.github.reata.sqllineage4j.common.model;

import java.util.Objects;

import static io.github.reata.sqllineage4j.common.utils.Helper.escapeIdentifierName;

public final class Schema {
    private final String rawName;

    public Schema() {
        rawName = "<default>";
    }

    public Schema(String name) {
        rawName = escapeIdentifierName(name);
    }

    @Override
    public String toString() {
        return rawName.toLowerCase();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Schema && this.toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }
}
