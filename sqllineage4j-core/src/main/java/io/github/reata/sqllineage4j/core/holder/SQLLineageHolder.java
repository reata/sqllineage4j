package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Table;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.javatuples.Pair;

import java.util.Set;
import java.util.stream.Collectors;

public class SQLLineageHolder {
    private final Graph graph;

    public SQLLineageHolder(Graph graph) {
        this.graph = graph;
    }

    public Set<Table> getSourceTables() {
        GraphTraversalSource g = graph.traversal();
        Set<Table> sourceTables = g.V().where(__.outE()).not(__.inE()).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> sourceOnlyTables = g.V().has("source_only", true).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> selfLoopTables = g.V().has("selfloop", true).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        sourceTables.addAll(sourceOnlyTables);
        sourceTables.addAll(selfLoopTables);
        return sourceTables;
    }

    public Set<Table> getTargetTables() {
        GraphTraversalSource g = graph.traversal();
        Set<Table> targetTables = g.V().where(__.inE()).not(__.outE()).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> targetOnlyTables = g.V().has("target_only", true).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> selfLoopTables = g.V().has("selfloop", true).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        targetTables.addAll(targetOnlyTables);
        targetTables.addAll(selfLoopTables);
        return targetTables;
    }

    public static SQLLineageHolder of(StatementLineageHolder... statementLineageHolders) {
        Graph graph = buildDiGraph(statementLineageHolders);
        return new SQLLineageHolder(graph);
    }

    private static Graph buildDiGraph(StatementLineageHolder... statementLineageHolders) {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();
        for (StatementLineageHolder holder : statementLineageHolders) {
            graph = compose(graph, holder.getGraph());
            g = graph.traversal();
            if (holder.getDrop().size() > 0) {
                g.V().hasId(holder.getDrop().stream().map(Table::hashCode).toArray()).not(__.bothE()).drop().iterate();
            } else if (holder.getRename().size() > 0) {
                for (Pair<Table, Table> p : holder.getRename()) {
                    Table tableOld = p.getValue0();
                    Table tableNew = p.getValue1();
                    for (Object targetId : g.V().hasId(tableOld.hashCode()).outE().otherV().id().toList()) {
                        g.V().hasId(tableNew.hashCode()).as("src")
                                .V().hasId(targetId).as("tgt")
                                .coalesce(__.inE("lineage").where(__.otherV().is("src")),
                                        __.addE("lineage").from("src")).next();
                    }
                    for (Object sourceId : g.V().hasId(tableOld.hashCode()).inE().otherV().id().toList()) {
                        g.V().hasId(sourceId).as("src")
                                .V().hasId(tableNew.hashCode()).as("tgt")
                                .coalesce(__.inE("lineage").where(__.otherV().is("src")),
                                        __.addE("lineage").from("src")).next();
                    }
                    g.V().hasId(tableOld.hashCode()).drop().iterate();
                    g.V().hasId(tableNew.hashCode()).not(__.bothE()).drop().iterate();
                }
            } else {
                Set<Table> read = holder.getRead();
                Set<Table> write = holder.getWrite();
                if (read.size() > 0 && write.size() == 0) {
                    // source only table comes from SELECT statement
                    g.V().hasId(read.stream().map(Table::hashCode).toArray()).property("source_only", true).iterate();
                } else if (read.size() == 0 && write.size() > 0) {
                    // target only table comes from case like: 1) INSERT/UPDATE constant values; 2) CREATE TABLE
                    g.V().hasId(write.stream().map(Table::hashCode).toArray()).property("target_only", true).iterate();
                } else {
                    for (Table r : read) {
                        for (Table w : write) {
                            g.V().hasId(r.hashCode()).as("src")
                                    .V().hasId(w.hashCode()).as("tgt")
                                    .coalesce(__.inE("lineage").where(__.otherV().is("src")),
                                            __.addE("lineage").from("src")).next();
                        }
                    }
                }
            }
        }
        g.V().as("src").where(__.outE().otherV().as("src")).fold()
                .coalesce(__.unfold().property("selfloop", true), __.identity()).iterate();
        return graph;
    }

    private static Graph compose(Graph a, Graph b) {
        GraphTraversalSource ag = a.traversal();
        GraphTraversalSource bg = b.traversal();
        Set<Table> bTables = bg.V().values("obj").toList().stream().map(Table.class::cast).collect(Collectors.toSet());
        for (Table bTable : bTables) {
            String label = bTable.getClass().getSimpleName();
            int id = bTable.hashCode();
            ag.V().hasLabel(label).hasId(id).fold()
                    .coalesce(__.unfold(), __.addV(label).property(T.id, id).property("obj", bTable)).next();
        }
        return a;
    }
}
