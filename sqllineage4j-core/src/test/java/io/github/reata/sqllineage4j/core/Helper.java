package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.model.Table;

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
        assertEquals(sourceTables.stream().map(Table::new).collect(Collectors.toSet()), Set.copyOf(runner.sourceTables()));
        assertEquals(targetTables.stream().map(Table::new).collect(Collectors.toSet()), Set.copyOf(runner.targetTables()));
    }
}
