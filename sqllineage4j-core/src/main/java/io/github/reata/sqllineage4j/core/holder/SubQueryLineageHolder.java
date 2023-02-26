package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.DataSet;
import io.github.reata.sqllineage4j.common.model.SubQuery;
import io.github.reata.sqllineage4j.graph.GremlinLineageGraph;
import io.github.reata.sqllineage4j.graph.LineageGraph;

import java.util.*;
import java.util.stream.Collectors;

public class SubQueryLineageHolder {
    final LineageGraph lineageGraph = new GremlinLineageGraph();

    public LineageGraph getGraph() {
        return lineageGraph;
    }

    void propertySetter(DataSet value, String prop) {
        lineageGraph.addVertexIfNotExist(value, Collections.singletonMap(prop, Boolean.TRUE));
    }

    Set<DataSet> propertyGetter(String prop) {
        return lineageGraph.retrieveVerticesByProps(Collections.singletonMap(prop, true))
                .stream().map(x -> (DataSet) x).collect(Collectors.toSet());
    }

    public Set<? extends DataSet> getRead() {
        return propertyGetter("read");
    }

    public Set<? extends DataSet> getWrite() {
        return propertyGetter("write");
    }

    public Set<SubQuery> getCTE() {
        return propertyGetter("cte").stream().map(x -> (SubQuery) x).collect(Collectors.toSet());
    }

    public void addRead(DataSet read) {
        propertySetter(read, "read");
    }

    public void addWrite(DataSet write) {
        propertySetter(write, "write");
    }

    public void addCTE(SubQuery cte) {
        propertySetter(cte, "cte");
    }

    public void addColumnLineage(Column src, Column tgt) {
        lineageGraph.addVertexIfNotExist(src);
        lineageGraph.addVertexIfNotExist(tgt);
        lineageGraph.addEdgeIfNotExist("lineage", src, tgt);
        lineageGraph.addEdgeIfNotExist("has_column", Objects.requireNonNull(tgt.getParent()), tgt);
        if (src.getParent() != null) {
            lineageGraph.addEdgeIfNotExist("has_column", Objects.requireNonNull(src.getParent()), src);
        }
    }

    @Override
    public String toString() {
        return "table read: " + getRead().toString() + "\n" +
                "table write: " + getWrite().toString() + "\n" +
                "table cte: " + getCTE().toString();
    }

    private final List<Column> selectColumns = new ArrayList<>();

    public List<Column> getSelectColumns() {
        return selectColumns;
    }
}
