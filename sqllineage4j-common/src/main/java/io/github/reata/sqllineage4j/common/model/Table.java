package io.github.reata.sqllineage4j.common.model;

import java.util.Objects;

import static io.github.reata.sqllineage4j.common.utils.Helper.escapeIdentifierName;

public final class Table implements DataSet {
    private final String rawName;
    private Schema schema = new Schema();

    public Table(String name) {
        if (name.contains(".")) {
            int pos = name.lastIndexOf(".");
            String schemaName = name.substring(0, pos + 1);
            String tableName = name.substring(pos + 1);
            this.schema = new Schema(schemaName);
            this.rawName = escapeIdentifierName(tableName);
        } else {
            this.rawName = escapeIdentifierName(name);
        }
    }

    @Override
    public String toString() {
        return schema.toString() + "." + rawName.toLowerCase();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Table && this.toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }
}
