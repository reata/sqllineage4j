package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.graph.GremlinLineageGraph;
import io.github.reata.sqllineage4j.graph.LineageGraph;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class StatementLineageHolder {
    private final LineageGraph lineageGraph = new GremlinLineageGraph();

    public LineageGraph getGraph() {
        return lineageGraph;
    }

    private void propertySetter(Table value, String prop) {
        lineageGraph.addVertexIfNotExist(value, Collections.singletonMap(prop, Boolean.TRUE));
    }

    private Set<Table> propertyGetter(String prop) {
        return lineageGraph.retrieveVerticesByProps(Collections.singletonMap(prop, true))
                .stream().map(Table.class::cast).collect(Collectors.toSet());
    }

    public Set<Table> getRead() {
        // FIXME: SQLLineage use SubQuery to exclude CTE
        Set<Table> read = propertyGetter("read");
        read.removeAll(getCTE());
        return read;
    }

    public Set<Table> getWrite() {
        return propertyGetter("write");
    }

    public Set<Table> getDrop() {
        return propertyGetter("drop");
    }

    public Set<Table> getCTE() {
        return propertyGetter("cte");
    }

    public Set<Pair<Table, Table>> getRename() {
        return lineageGraph.retrieveEdgesByLabel("rename").stream().map(
                e -> new Pair<>((Table) e.source(), (Table) e.target())
        ).collect(Collectors.toSet());
    }

    public void addRead(String read) {
        propertySetter(new Table(read), "read");
    }

    public void addWrite(String write) {
        propertySetter(new Table(write), "write");
    }

    public void addDrop(String drop) {
        propertySetter(new Table(drop), "drop");
    }

    public void addCTE(String intermediate) {
        propertySetter(new Table(intermediate), "cte");
    }

    public void addRename(String src, String tgt) {
        Table source = new Table(src);
        Table target = new Table(tgt);
        lineageGraph.addVertexIfNotExist(source);
        lineageGraph.addVertexIfNotExist(target);
        lineageGraph.addEdgeIfNotExist("rename", source, target);
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
                "table cte: " + getCTE().toString() + "\n" +
                "table drop: " + getDrop().toString() + "\n" +
                "table cte: " + getRename().toString();
    }
}
