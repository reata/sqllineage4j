package io.github.reata.sqllineage4j.common.model;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;

import javax.annotation.Nullable;
import java.util.*;

import static io.github.reata.sqllineage4j.common.utils.Helper.escapeIdentifierName;


public class Column {
    private final Set<Table> parent = new HashSet<>();
    private final String rawName;
    private final List<ColumnQualifierTuple> sourceColumns = new ArrayList<>();

    public Column(String name) {
        this.rawName = escapeIdentifierName(name);
    }

    @Override
    public String toString() {
        if (getParent() != null) {
            return getParent().toString() + "." + rawName.toLowerCase();
        } else {
            return rawName.toLowerCase();
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Column && this.toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }

    public @Nullable Table getParent() {
        return parent.size() == 1 ? List.copyOf(parent).get(0) : null;
    }

    public void setParent(Table table) {
        parent.add(table);
    }

    public void setSourceColumns(ColumnQualifierTuple cqt) {
        sourceColumns.add(cqt);
    }

    public List<Column> toSourceColumns(Map<String, Table> aliasMapping) {
        List<Column> result = new ArrayList<>();
        for (ColumnQualifierTuple columnQualifierTuple : sourceColumns) {
            if (columnQualifierTuple.qualifier() == null) {
                Column source = new Column(columnQualifierTuple.column());
                for (Table table : aliasMapping.values()) {
                    // in case of only one table, we get the right answer
                    // in case of multiple tables, a bunch of possible tables are set
                    source.setParent(table);
                }
                result.add(source);
            } else {

            }
        }
        return result;
    }
}
