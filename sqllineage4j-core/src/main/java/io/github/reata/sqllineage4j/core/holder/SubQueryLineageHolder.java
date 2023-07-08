package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.entity.EdgeTuple;
import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.QuerySet;
import io.github.reata.sqllineage4j.common.model.SubQuery;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.graph.GremlinLineageGraph;
import io.github.reata.sqllineage4j.graph.LineageGraph;

import java.util.*;
import java.util.stream.Collectors;

public class SubQueryLineageHolder {
    final LineageGraph lineageGraph = new GremlinLineageGraph();

    public LineageGraph getGraph() {
        return lineageGraph;
    }

    void propertySetter(QuerySet value, String prop) {
        lineageGraph.addVertexIfNotExist(value, Collections.singletonMap(prop, Boolean.TRUE));
    }

    Set<QuerySet> propertyGetter(String prop) {
        return lineageGraph.retrieveVerticesByProps(Collections.singletonMap(prop, true))
                .stream().map(x -> (QuerySet) x).collect(Collectors.toSet());
    }

    public Set<? extends QuerySet> getRead() {
        return propertyGetter("read");
    }

    public Set<? extends QuerySet> getWrite() {
        return propertyGetter("write");
    }

    public Set<SubQuery> getCTE() {
        return propertyGetter("cte").stream().map(x -> (SubQuery) x).collect(Collectors.toSet());
    }

    public void addRead(QuerySet read) {
        propertySetter(read, "read");
        if (read.getAlias() != null) {
            lineageGraph.addVertexIfNotExist(read.getAlias());
            lineageGraph.addEdgeIfNotExist("has_alias", read, read.getAlias());
        }
    }

    public void addWrite(QuerySet write) {
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

    public Map<String, QuerySet> getQuerySetAlias() {
        Map<String, QuerySet> aliasMapping = new HashMap<>();
        for (EdgeTuple edgeTuple : lineageGraph.retrieveEdgesByLabel("has_alias")) {
            aliasMapping.put((String) edgeTuple.target(), (QuerySet) edgeTuple.source());
        }
        return aliasMapping;
    }
}
