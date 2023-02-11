package io.github.reata.sqllineage4j.graph;

import io.github.reata.sqllineage4j.common.entity.EdgeTuple;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GremlinLineageGraph implements LineageGraph {
    private final GraphTraversalSource g;

    public GremlinLineageGraph() {
        this.g = TinkerGraph.open().traversal();
    }

    public GremlinLineageGraph(Graph graph) {
        this.g = graph.traversal();
    }

    public void addVertexIfNotExist(Object obj) {
        HashMap<String, Object> props = new HashMap<>();
        addVertexIfNotExist(obj, props);
    }

    public void addVertexIfNotExist(Object obj, Map<String, Object> props) {
        int id = obj.hashCode();
        String label = obj.getClass().getSimpleName();
        GraphTraversal<Vertex, Vertex> step = g.V().hasLabel(label).hasId(id).fold()
                .coalesce(__.unfold(),
                        __.addV(label).property(T.id, id))
                .property("obj", obj);
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            step = step.property(entry.getKey(), entry.getValue());
        }
        step.iterate();
    }

    public List<Object> retrieveVerticesByProps(Map<String, Object> props) {
        GraphTraversal<Vertex, Vertex> step = g.V();
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            step = step.has(entry.getKey(), entry.getValue());
        }
        return step.values("obj").toList();
    }

    public List<Object> retrieveSourceOnlyVertices() {
        return retrieveVertices(g.V().where(__.outE()).not(__.inE()));
    }

    public List<Object> retrieveTargetOnlyVertices() {
        return retrieveVertices(g.V().where(__.inE()).not(__.outE()));
    }

    public List<Object> retrieveConnectedVertices() {
        return retrieveVertices(g.V().where(__.inE()).where(__.outE()));
    }

    public List<Object> retrieveSelfLoopVertices() {
        return retrieveVertices(g.V().as("src").where(__.outE().otherV().as("src")));
    }

    private List<Object> retrieveVertices(GraphTraversal<Vertex, Vertex> vertexStep) {
        return vertexStep.values("obj").toList();
    }

    public void updateVertices(Map<String, Object> props, Object... objects) {
        GraphTraversal<Vertex, Vertex> step = g.V().hasId(Arrays.stream(objects).map(Object::hashCode).toArray());
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            step = step.property(entry.getKey(), entry.getValue());
        }
        step.iterate();
    }

    public void dropVertices(Object... objects) {
        dropVertices(false, objects);
    }

    public void dropVerticesIfOrphan(Object... objects) {
        dropVertices(true, objects);
    }

    private void dropVertices(boolean orphan, Object... objects) {
        GraphTraversal<Vertex, Vertex> step = g.V().hasId(Arrays.stream(objects).map(Object::hashCode).toArray());
        if (orphan) {
            step = step.not(__.bothE());
        }
        step.drop().iterate();
    }

    public void addEdgeIfNotExist(String label, Object src, Object tgt) {
        g.V().hasLabel(src.getClass().getSimpleName()).hasId(src.hashCode()).as("src")
                .V().hasLabel(tgt.getClass().getSimpleName()).hasId(tgt.hashCode())
                .coalesce(__.inE(label).where(__.outV().as("src")),
                        __.addE(label).from("src")).iterate();
    }

    public List<EdgeTuple> retrieveEdgesByProps(Map<String, Object> props) {
        GraphTraversal<Edge, Edge> step = g.E();
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            step = step.has(entry.getKey(), entry.getValue());
        }
        return retrieveEdges(step);
    }

    public List<EdgeTuple> retrieveEdgesByLabel(String label) {
        return retrieveEdges(g.E().hasLabel(label));
    }

    public List<EdgeTuple> retrieveEdgesByVertex(Object object) {
        return retrieveEdges(g.V().hasId(object.hashCode()).bothE());
    }

    private List<EdgeTuple> retrieveEdges(GraphTraversal<? extends Element, Edge> edgeStep) {
        return edgeStep.project("from", "label", "to")
                .by(__.outV().values("obj"))
                .by(__.label())
                .by(__.inV().values("obj")).toList()
                .stream().map(x -> EdgeTuple.create(x.get("from"), (String) x.get("label"), x.get("to"))).collect(Collectors.toList());
    }

    public void dropSelfLoopEdge() {
        g.V().as("src").outE().as("e").inV()
                .where(P.eq("src")).inE().where(P.eq("e")).drop().iterate();
    }

    public GremlinLineageGraph getSubGraph(String label) {
        return new GremlinLineageGraph((Graph) g.E().where(__.inV().hasLabel(label)).where(__.outV().hasLabel(label))
                .subgraph("sg").cap("sg").next());
    }
}
