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
    public void testSelectColumnUsingWindowFunction() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT row_number() OVER (PARTITION BY col1 ORDER BY col2 DESC) AS rnum\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("rnum", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("rnum", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingWindowFunctionWithParameters() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col0,\n" +
                        "       max(col3) OVER (PARTITION BY col1 ORDER BY col2 DESC) AS rnum,\n" +
                        "       col4\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col0", "tab2"),
                                ColumnQualifierTuple.create("col0", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("rnum", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("rnum", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("rnum", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col4", "tab2"),
                                ColumnQualifierTuple.create("col4", "tab1"))));
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

    @Test
    public void testSelectColumnUsingExpression() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1 + col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1 + col2", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col1 + col2", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1 + col2 AS col3\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingExpressionInParenthesis() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT (col1 + col2) AS col3\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingBooleanExpressionInParenthesis() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT (col1 > 0 AND col2 > 0) AS col3\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingExpressionWithTableQualifierWithoutColumnAlias() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT a.col1 + a.col2 + a.col3 + a.col4\n" +
                        "FROM tab2 a",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("a.col1 + a.col2 + a.col3 + a.col4", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("a.col1 + a.col2 + a.col3 + a.col4", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("a.col1 + a.col2 + a.col3 + a.col4", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col4", "tab2"),
                                ColumnQualifierTuple.create("a.col1 + a.col2 + a.col3 + a.col4", "tab1"))));
    }

    @Test
    public void testSelectColumnUsingCaseWhen() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END AS col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col2", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' ELSE col_v END\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' ELSE col_v END", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col_v", "tab2"),
                                ColumnQualifierTuple.create("CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' ELSE col_v END", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' ELSE col_v END AS col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col_v", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1"))));
    }

//    @Test
//    public void testSelectColumnUsingCaseWhenWithSubquery() {
//        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
//                        "SELECT CASE WHEN (SELECT avg(col1) FROM tab3) > 0 AND col2 = 1 THEN (SELECT avg(col1) FROM tab3) ELSE 0 END AS col1\n" +
//                        "FROM tab4",
//                Set.of(Pair.with(ColumnQualifierTuple.create("col2", "tab4"),
//                                ColumnQualifierTuple.create("col1", "tab1")),
//                        Pair.with(ColumnQualifierTuple.create("col1", "tab3"),
//                                ColumnQualifierTuple.create("col1", "tab1"))));
//    }

    @Test
    public void testSelectColumnWithTableQualifier() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT tab2.col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT t.col1\n" +
                        "FROM tab2 AS t",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumns() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1,\n" +
                        "col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT max(col1),\n" +
                        "max(col2)\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("max(col1)", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("max(col2)", "tab1"))));
    }

    @Test
    public void testSelectColumnInSubquery() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (SELECT col1 FROM tab2) dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (SELECT col1, col2 FROM tab2) dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }
}
