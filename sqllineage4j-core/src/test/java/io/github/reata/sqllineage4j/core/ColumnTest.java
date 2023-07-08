package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
//        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
//                        "SELECT tab2.col1\n" +
//                        "FROM tab2",
//                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
//                        ColumnQualifierTuple.create("col1", "tab1"))));
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
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (SELECT col1 FROM tab2)",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnInSubqueryWithTwoParenthesis() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM ((SELECT col1 FROM tab2)) dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnInSubqueryWithTwoParenthesisAndBlankInBetween() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (\n" +
                        "(SELECT col1 FROM tab2)\n" +
                        ") dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnInSubqueryWithTwoParenthesisAndUnion() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (\n" +
                        "    (SELECT col1 FROM tab2)\n" +
                        "    UNION ALL\n" +
                        "    (SELECT col1 FROM tab3)\n" +
                        ") dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col1", "tab3"),
                                ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnInSubqueryWithTwoParenthesisAndUnionV2() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM (\n" +
                        "    SELECT col1 FROM tab2\n" +
                        "    UNION ALL\n" +
                        "    SELECT col1 FROM tab3\n" +
                        ") dt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col1", "tab3"),
                                ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnWithoutTableQualifierFromTableJoin() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT col1\n" +
                        "FROM tab2 a\n" +
                        "         INNER JOIN tab3 b\n" +
                        "                    ON a.id = b.id",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", null),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testSelectColumnFromSameTableMultipleTimeUsingDifferentAlias() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT a.col1 AS col2,\n" +
                        "       b.col1 AS col3\n" +
                        "FROM tab2 a\n" +
                        "         JOIN tab2 b\n" +
                        "              ON a.parent_id = b.id",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testCommentAfterColumnCommaFirst() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT a.col1\n" +
                        "       --, a.col2\n" +
                        "       , a.col3\n" +
                        "FROM tab2 a",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testCommentAfterColumnCommaLast() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT a.col1,\n" +
                        "       -- a.col2,\n" +
                        "       a.col3\n" +
                        "FROM tab2 a",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("col3", "tab1"))));
    }

    @Test
    public void testCastWithComparison() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(col1 = 1 AS int) col1, col2 = col3 col2\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("col2", "tab1"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"string", "timestamp", "date", "datetime", "decimal(18, 0)"})
    public void testCastToDataType(String dtype) {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(col1 as " + dtype + ") AS col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"string", "timestamp", "date", "datetime", "decimal(18, 0)"})
    public void testNestedCastToDataType(String dtype) {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(cast(col1 AS " + dtype + ") AS " + dtype + ") AS col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(cast(cast(cast(cast(col1 AS " + dtype + ") AS " + dtype + ") AS " + dtype + ") AS " + dtype + ") AS " + dtype + ") AS col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"string", "timestamp", "date", "datetime", "decimal(18, 0)"})
    public void testCastToDataTypeWithCaseWhen(String dtype) {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast(case when col1 > 0 then col2 else col3 end as " + dtype + ") AS col1\n" +
                        "FROM tab2",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col3", "tab2"),
                                ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testCastUsingConstant() {
        assertColumnLineage("INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT cast('2012-12-21' as date) AS col2",
                Set.of());
    }

    @Test
    public void testWindowFunctionInSubquery() {
        assertColumnLineage("INSERT INTO tab1\n" +
                        "SELECT rn FROM (\n" +
                        "    SELECT\n" +
                        "        row_number() OVER (PARTITION BY col1, col2) rn\n" +
                        "    FROM tab2\n" +
                        ") sub\n" +
                        "WHERE rn = 1",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                                ColumnQualifierTuple.create("rn", "tab1")),
                        Pair.with(ColumnQualifierTuple.create("col2", "tab2"),
                                ColumnQualifierTuple.create("rn", "tab1"))));
    }

    @Test
    public void testInvalidSyntaxAsWithoutAlias() {
        String sql = "INSERT OVERWRITE TABLE tab1\n" +
                "SELECT col1,\n" +
                "       col2 as,\n" +
                "       col3\n" +
                "FROM tab2";
        // just assure no exception, don't guarantee the result
        LineageRunner runner = LineageRunner.builder(sql).build();
        runner.getColumnLineage();
    }

    @Test
    public void testColumnReferenceFromCteUsingAlias() {
        assertColumnLineage("WITH wtab1 AS (SELECT col1 FROM tab2)\n" +
                        "INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT wt.col1 FROM wtab1 wt",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testColumnReferenceFromCteUsingQualifier() {
        assertColumnLineage("WITH wtab1 AS (SELECT col1 FROM tab2)\n" +
                        "INSERT OVERWRITE TABLE tab1\n" +
                        "SELECT wtab1.col1 FROM wtab1",
                Set.of(Pair.with(ColumnQualifierTuple.create("col1", "tab2"),
                        ColumnQualifierTuple.create("col1", "tab1"))));
    }

    @Test
    public void testColumnReferenceFromPreviousDefinedCte() {
        assertColumnLineage("WITH\n" +
                        "cte1 AS (SELECT a FROM tab1),\n" +
                        "cte2 AS (SELECT a FROM cte1)\n" +
                        "INSERT OVERWRITE TABLE tab2\n" +
                        "SELECT a FROM cte2",
                Set.of(Pair.with(ColumnQualifierTuple.create("a", "tab1"),
                        ColumnQualifierTuple.create("a", "tab2"))));
    }

    @Test
    public void testMultipleColumnReferencesFromPreviousDefinedCte() {
        assertColumnLineage("WITH\n" +
                        "cte1 AS (SELECT a, b FROM tab1),\n" +
                        "cte2 AS (SELECT a, max(b) AS b_max, count(b) AS b_cnt FROM cte1 GROUP BY a)\n" +
                        "INSERT OVERWRITE TABLE tab2\n" +
                        "SELECT cte1.a, cte2.b_max, cte2.b_cnt FROM cte1 JOIN cte2\n" +
                        "WHERE cte1.a = cte2.a",
                Set.of(Pair.with(ColumnQualifierTuple.create("a", "tab1"),
                                ColumnQualifierTuple.create("a", "tab2")),
                        Pair.with(ColumnQualifierTuple.create("b", "tab1"),
                                ColumnQualifierTuple.create("b_max", "tab2")),
                        Pair.with(ColumnQualifierTuple.create("b", "tab1"),
                                ColumnQualifierTuple.create("b_cnt", "tab2"))));
    }
}
