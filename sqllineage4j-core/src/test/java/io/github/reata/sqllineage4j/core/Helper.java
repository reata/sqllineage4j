package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;
import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.Table;
import org.javatuples.Pair;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class Helper {

    public static void assertTableLineage(String sql) {
        assertTableLineage(sql, Set.of(), Set.of());
    }

    public static void assertTableLineage(String sql, Set<String> sourceTables) {
        assertTableLineage(sql, sourceTables, Set.of());
    }

    public static void assertTableLineage(String sql, Set<String> sourceTables, Set<String> targetTables) {
        LineageRunner runner = LineageRunner.builder(sql).build();
        assertEquals("Source Table Equal", sourceTables.stream().map(Table::new).collect(Collectors.toSet()), Set.copyOf(runner.sourceTables()));
        assertEquals("Target Table Equal", targetTables.stream().map(Table::new).collect(Collectors.toSet()), Set.copyOf(runner.targetTables()));
    }

    public static void assertColumnLineage(String sql, Set<Pair<ColumnQualifierTuple, ColumnQualifierTuple>> columnLineages) {
        Set<Pair<Column, Column>> expected = new HashSet<>();
        for (Pair<ColumnQualifierTuple, ColumnQualifierTuple> cqtPair : columnLineages) {
            ColumnQualifierTuple srcCqt = cqtPair.getValue0();
            ColumnQualifierTuple tgtCqt = cqtPair.getValue1();
            Column srcCol = new Column(srcCqt.column());
            if (srcCqt.qualifier() != null) {
                srcCol.setParent(new Table(srcCqt.qualifier()));
            }
            Column tgtCol = new Column(tgtCqt.column());
            tgtCol.setParent(new Table(Objects.requireNonNull(tgtCqt.qualifier())));
            expected.add(Pair.with(srcCol, tgtCol));
        }
        LineageRunner runner = LineageRunner.builder(sql).build();
        Set<Pair<Column, Column>> actual = new HashSet<>(runner.getColumnLineage());
        assertEquals(expected, actual);
    }
}
