package io.github.reata.sqllineage4j.core.holder;

import io.github.reata.sqllineage4j.common.model.Table;
import org.javatuples.Pair;

import java.util.Set;
import java.util.stream.Collectors;

public class StatementLineageHolder extends SubQueryLineageHolder {

    @Override
    public Set<Table> getRead() {
        return super.getRead().stream().filter(x -> x instanceof Table).map(x -> (Table) x).collect(Collectors.toSet());
    }

    @Override
    public Set<Table> getWrite() {
        return super.getWrite().stream().filter(x -> x instanceof Table).map(x -> (Table) x).collect(Collectors.toSet());
    }

    public Set<Table> getDrop() {
        return propertyGetter("drop").stream().map(x -> (Table) x).collect(Collectors.toSet());
    }

    public Set<Pair<Table, Table>> getRename() {
        return lineageGraph.retrieveEdgesByLabel("rename").stream().map(
                e -> new Pair<>((Table) e.source(), (Table) e.target())
        ).collect(Collectors.toSet());
    }

    public void addDrop(Table drop) {
        propertySetter(drop, "drop");
    }

    public void addRename(Table src, Table tgt) {
        lineageGraph.addVertexIfNotExist(src);
        lineageGraph.addVertexIfNotExist(tgt);
        lineageGraph.addEdgeIfNotExist("rename", src, tgt);
    }

    @Override
    public String toString() {
        return super.toString() +
                "table drop: " + getDrop().toString() + "\n" +
                "table cte: " + getRename().toString();
    }

    public void union(SubQueryLineageHolder holder) {
        getGraph().merge(holder.getGraph());
    }
}
