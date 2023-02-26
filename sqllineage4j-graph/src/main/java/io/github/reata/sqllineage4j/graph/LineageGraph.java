package io.github.reata.sqllineage4j.graph;

import io.github.reata.sqllineage4j.common.entity.EdgeTuple;

import java.util.List;
import java.util.Map;

public interface LineageGraph {

    void addVertexIfNotExist(Object obj);

    void addVertexIfNotExist(Object obj, Map<String, Object> props);

    List<Object> retrieveVerticesByProps(Map<String, Object> props);

    List<Object> retrieveSourceOnlyVertices();

    List<Object> retrieveTargetOnlyVertices();

    List<Object> retrieveConnectedVertices();

    List<Object> retrieveSelfLoopVertices();

    void updateVertices(Map<String, Object> props, Object... objects);

    void dropVertices(Object... objects);

    void dropVerticesIfOrphan(Object... objects);

    void addEdgeIfNotExist(String label, Object src, Object tgt);

    List<EdgeTuple> retrieveEdgesByProps(Map<String, Object> props);

    List<EdgeTuple> retrieveEdgesByLabel(String label);

    List<EdgeTuple> retrieveEdgesByVertex(Object object);

    void dropSelfLoopEdge();

    LineageGraph getSubGraph(String label);

    void merge(LineageGraph other);

    List<List<Object>> listPath(Object source, Object target);
}
