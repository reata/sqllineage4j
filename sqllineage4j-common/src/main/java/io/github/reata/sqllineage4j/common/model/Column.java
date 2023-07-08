package io.github.reata.sqllineage4j.common.model;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;

import javax.annotation.Nullable;
import java.util.*;

import static io.github.reata.sqllineage4j.common.utils.Helper.escapeIdentifierName;


public class Column {
    private final Set<QuerySet> parent = new HashSet<>();
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

    public @Nullable QuerySet getParent() {
        return parent.size() == 1 ? List.copyOf(parent).get(0) : null;
    }

    public void setParent(QuerySet table) {
        parent.add(table);
    }

    public void setSourceColumns(ColumnQualifierTuple cqt) {
        sourceColumns.add(cqt);
    }

    public List<Column> toSourceColumns(Map<String, QuerySet> aliasMapping) {
        List<Column> sourceColumns = new ArrayList<>();
        for (ColumnQualifierTuple columnQualifierTuple : this.sourceColumns) {
            String srcCol = columnQualifierTuple.column();
            String qualifier = columnQualifierTuple.qualifier();
            if (qualifier == null) {
                if (srcCol.equals("*")) {
                    // SELECT *
                    for (QuerySet dataSet : aliasMapping.values()) {
                        sourceColumns.add(toSourceColumn(srcCol, dataSet));
                    }
                } else {
                    // select unqualified column
                    Column source = new Column(srcCol);
                    for (QuerySet dataSet : aliasMapping.values()) {
                        // in case of only one table, we get the right answer
                        // in case of multiple tables, a bunch of possible tables are set
                        source.setParent(dataSet);
                    }
                    sourceColumns.add(source);
                }
            } else {
                if (aliasMapping.containsKey(qualifier)) {
                    sourceColumns.add(toSourceColumn(srcCol, aliasMapping.get(qualifier)));
                } else {
                    sourceColumns.add(toSourceColumn(srcCol, new Table(qualifier)));
                }
            }
        }
        return sourceColumns;
    }

    private Column toSourceColumn(String columnName, QuerySet parent) {
        Column col = new Column(columnName);
        if (parent != null) {
            col.setParent(parent);
        }
        return col;
    }
}
