package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Table;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Set;
import java.util.stream.Collectors;

public class StatementLineageHolder {
    private final Graph graph = TinkerGraph.open();
    private final GraphTraversalSource g = graph.traversal();

    public Graph getGraph() {
        return graph;
    }

    private void propertySetter(Table value, String prop) {
        String label = value.getClass().getSimpleName();
        int id = value.hashCode();
        g.V().hasLabel(label).hasId(id).fold()
                .coalesce(__.unfold(), __.addV(label).property(T.id, id))
                .property(prop, true)
                .property("obj", value)
                .next();
    }

    private Set<Table> propertyGetter(String prop) {
        return g.V().has(prop, true).values("obj").toList()
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
}
