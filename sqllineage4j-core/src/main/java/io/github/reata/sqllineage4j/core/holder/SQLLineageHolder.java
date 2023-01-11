package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.Table;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.javatuples.Pair;

import java.util.Set;
import java.util.stream.Collectors;

public class SQLLineageHolder {
    private final GraphTraversalSource g;

    public SQLLineageHolder(Graph graph) {
        this.g = graph.traversal();
    }

    public Set<Table> getSourceTables() {
        Set<Table> sourceTables = getTableLineageGraph().V().where(__.outE()).not(__.inE()).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> sourceOnlyTables = retrieveTagTables("source_only");
        Set<Table> selfLoopTables = retrieveTagTables("selfloop");
        sourceTables.addAll(sourceOnlyTables);
        sourceTables.addAll(selfLoopTables);
        return sourceTables;
    }

    public Set<Table> getTargetTables() {
        Set<Table> targetTables = getTableLineageGraph().V().where(__.inE()).not(__.outE()).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        Set<Table> targetOnlyTables = retrieveTagTables("target_only");
        Set<Table> selfLoopTables = retrieveTagTables("selfloop");
        targetTables.addAll(targetOnlyTables);
        targetTables.addAll(selfLoopTables);
        return targetTables;
    }

    public Set<Table> getIntermediateTables() {
        Set<Table> intermediateTables = getTableLineageGraph().V().where(__.inE()).where(__.outE()).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
        intermediateTables.removeAll(retrieveTagTables("selfloop"));
        return intermediateTables;
    }

    public Set<Pair<Column, Column>> getColumnLineage(boolean excludeSubquery) {
        return getColumnLineageGraph().E().where(__.outV().hasLabel(Column.class.getSimpleName())).project("from", "to")
                .by(__.outV().values("obj"))
                .by(__.inV().values("obj"))
                .toList()
                .stream().map(x -> Pair.with((Column) x.get("from"), (Column) x.get("to")))
                .collect(Collectors.toSet());
    }

    private GraphTraversalSource getTableLineageGraph() {
        String label = Table.class.getSimpleName();
        Graph s = (Graph) g.E().where(__.inV().hasLabel(label)).where(__.outV().hasLabel(label)).subgraph("tg").cap("tg").next();
        return s.traversal();
    }

    private GraphTraversalSource getColumnLineageGraph() {
        String label = Column.class.getSimpleName();
        Graph s = (Graph) g.E().where(__.inV().hasLabel(label)).where(__.outV().hasLabel(label)).subgraph("cg").cap("cg").next();
        return s.traversal();
    }

    private Set<Table> retrieveTagTables(String tag) {
        return g.V().hasLabel(Table.class.getSimpleName()).has(tag, true).values("obj").toList()
                .stream().map(Table.class::cast).collect(Collectors.toSet());
    }

    public static SQLLineageHolder of(StatementLineageHolder... statementLineageHolders) {
        Graph graph = buildDiGraph(statementLineageHolders);
        return new SQLLineageHolder(graph);
    }

    private static Graph buildDiGraph(StatementLineageHolder... statementLineageHolders) {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();
        for (StatementLineageHolder holder : statementLineageHolders) {
            compose(graph, holder.getGraph());
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
                    g.V().as("src").outE().as("e").inV().where(P.eq("src")).inE().where(P.eq("e")).drop().iterate();
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

    private static void compose(Graph a, Graph b) {
        GraphTraversalSource ag = a.traversal();
        GraphTraversalSource bg = b.traversal();
        String tableLabel = Table.class.getSimpleName();
        Set<Table> bTables = bg.V().hasLabel(tableLabel).values("obj").toList().stream().map(Table.class::cast).collect(Collectors.toSet());
        for (Table bTable : bTables) {
            int id = bTable.hashCode();
            ag.V().hasLabel(tableLabel).hasId(id).fold()
                    .coalesce(__.unfold(), __.addV(tableLabel).property(T.id, id).property("obj", bTable)).next();
        }
        String columnLabel = Column.class.getSimpleName();
        Set<Column> bColumns = bg.V().hasLabel(columnLabel).values("obj").toList().stream().map(Column.class::cast).collect(Collectors.toSet());
        for (Column bColumn : bColumns) {
            int id = bColumn.hashCode();
            ag.V().hasLabel(columnLabel).hasId(id).fold()
                    .coalesce(__.unfold(), __.addV(columnLabel).property(T.id, id).property("obj", bColumn)).next();
        }
        bg.E().project("from_id", "label", "to_id")
                .by(__.outV().id())
                .by(__.label())
                .by(__.inV().id())
                .toList()
                .forEach(e -> ag.V().hasId(e.get("from_id")).as("src")
                        .V().hasId(e.get("to_id")).as("tgt")
                        .addE((String) e.get("label")).from("src").to("tgt").iterate());
    }
}
