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

    @Test
    public void testSelectColumnUsingFunction() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT max(col1),\n" +
                        "       count(*)\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("max(col1)", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("*", "tab2"),
                                ColumnQualifierTuple.create("count(*)", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT max(col1) AS col2,\n" +
                        "       count(*)  AS cnt\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("*", "tab2"),
                                ColumnQualifierTuple.create("cnt", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingFunctionWithComplexParameter() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT if(col1 = 'foo' AND col2 = 'bar', 1, 0) AS flag\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("flag", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("flag", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingCast() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(col1 as timestamp)\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("cast(col1 as timestamp)", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(col1 as timestamp) as col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col2", "tab1"))));
    }
}
