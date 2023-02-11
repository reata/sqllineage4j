package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.entity.EdgeTuple;
import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.graph.GremlinLineageGraph;
import io.github.reata.sqllineage4j.graph.LineageGraph;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLLineageHolder {
    private final LineageGraph lineageGraph;

    public SQLLineageHolder(LineageGraph lineageGraph) {
        this.lineageGraph = lineageGraph;
    }

    public Set<Table> getSourceTables() {
        LineageGraph tableLineageGraph = getTableLineageGraph();
        Set<Table> sourceTables = tableLineageGraph.retrieveSourceOnlyVertices()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> sourceOnlyTables = retrieveTagTables("source_only");
        Set<Table> selfLoopTables = retrieveTagTables("selfloop");
        sourceTables.addAll(sourceOnlyTables);
        sourceTables.addAll(selfLoopTables);
        return sourceTables;
    }

    public Set<Table> getTargetTables() {
        LineageGraph tableLineageGraph = getTableLineageGraph();
        Set<Table> targetTables = tableLineageGraph.retrieveTargetOnlyVertices()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> targetOnlyTables = retrieveTagTables("target_only");
        Set<Table> selfLoopTables = retrieveTagTables("selfloop");
        targetTables.addAll(targetOnlyTables);
        targetTables.addAll(selfLoopTables);
        return targetTables;
    }

    public Set<Table> getIntermediateTables() {
        LineageGraph tableLineageGraph = getTableLineageGraph();
        Set<Table> intermediateTables = tableLineageGraph.retrieveConnectedVertices()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        intermediateTables.removeAll(retrieveTagTables("selfloop"));
        return intermediateTables;
    }

    public Set<Pair<Column, Column>> getColumnLineage(boolean excludeSubquery) {
        LineageGraph columnLineageGraph = getColumnLineageGraph();
        return columnLineageGraph.retrieveEdgesByLabel("lineage")
                .stream().map(x -> Pair.with((Column) x.source(), (Column) x.target()))
                .collect(Collectors.toSet());
    }

    private LineageGraph getTableLineageGraph() {
        return lineageGraph.getSubGraph(Table.class.getSimpleName());
    }

    private LineageGraph getColumnLineageGraph() {
        return lineageGraph.getSubGraph(Column.class.getSimpleName());
    }

    private Set<Table> retrieveTagTables(String tag) {
        return lineageGraph.retrieveVerticesByProps(Collections.singletonMap(tag, true))
                .stream().map(Table.class::cast).collect(Collectors.toSet());
    }

    public static SQLLineageHolder of(StatementLineageHolder... statementLineageHolders) {
        LineageGraph graph = buildDiGraph(statementLineageHolders);
        return new SQLLineageHolder(graph);
    }

    private static LineageGraph buildDiGraph(StatementLineageHolder... statementLineageHolders) {
        LineageGraph lineageGraph = new GremlinLineageGraph();
        for (StatementLineageHolder holder : statementLineageHolders) {
            compose(lineageGraph, holder.getGraph());
            if (holder.getDrop().size() > 0) {
                lineageGraph.dropVerticesIfOrphan(holder.getDrop().toArray());
            } else if (holder.getRename().size() > 0) {
                for (Pair<Table, Table> p : holder.getRename()) {
                    Table tableOld = p.getValue0();
                    Table tableNew = p.getValue1();
                    for (EdgeTuple edgeTuple : lineageGraph.retrieveEdgesByVertex(tableOld)) {
                        if (edgeTuple.source().equals(tableOld)) {
                            lineageGraph.addEdgeIfNotExist(edgeTuple.label(), tableNew, edgeTuple.target());
                        } else if (edgeTuple.target().equals(tableOld)) {
                            lineageGraph.addEdgeIfNotExist(edgeTuple.label(), edgeTuple.source(), tableNew);
                        }
                    }
                    lineageGraph.dropVertices(tableOld);
                    lineageGraph.dropSelfLoopEdge();
                    lineageGraph.dropVerticesIfOrphan(tableNew);
                }
            } else {
                Set<Table> read = holder.getRead();
                Set<Table> write = holder.getWrite();
                if (read.size() > 0 && write.size() == 0) {
                    // source only table comes from SELECT statement
                    lineageGraph.updateVertices(Collections.singletonMap("source_only", Boolean.TRUE), read.toArray());
                } else if (read.size() == 0 && write.size() > 0) {
                    // target only table comes from case like: 1) INSERT/UPDATE constant values; 2) CREATE TABLE
                    lineageGraph.updateVertices(Collections.singletonMap("target_only", Boolean.TRUE), write.toArray());
                } else {
                    for (Table r : read) {
                        for (Table w : write) {
                            lineageGraph.addEdgeIfNotExist("lineage", r, w);
                        }
                    }
                }
            }
        }
        lineageGraph.updateVertices(Collections.singletonMap("selfloop", Boolean.TRUE),
                lineageGraph.retrieveSelfLoopVertices().stream().filter(x -> x instanceof Table).toArray());
        return lineageGraph;
    }

    private static void compose(LineageGraph a, LineageGraph b) {
        for (Object vertex : b.retrieveVerticesByProps(Collections.emptyMap())) {
            a.addVertexIfNotExist(vertex);
        }
        for (EdgeTuple edgeTuple : b.retrieveEdgesByProps(Collections.emptyMap())) {
            a.addEdgeIfNotExist(edgeTuple.label(), edgeTuple.source(), edgeTuple.target());
        }
    }
}
