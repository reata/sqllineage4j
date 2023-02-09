package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertColumnLineage;


public class ColumnTest {
    @Test
    public void testSelectColumn() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1 AS col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col2", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT tab2.col1 AS col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col2", "tab1"))));
    }

    @Test
    public void testSelectColumnWildcard() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT *\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("*", "tab2"),
                        ColumnQualifierTuple.create("*", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT *\n" +
                        "FROM tab2 a\n" +
                        "         INNER JOIN tab3 b\n" +
                        "                    ON a.id = b.id",
                Set.of(Pair.with(ColumnQualifierTuple.create("*", "tab2"),
                                ColumnQualifierTuple.create("*", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("*", "tab3"),
                                ColumnQualifierTuple.create("*", "tab1"))));
    }
}
