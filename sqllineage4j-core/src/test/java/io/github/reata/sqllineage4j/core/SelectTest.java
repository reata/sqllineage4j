package io.github.reata.sqllineage4j.core;

import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertTableLineage;

public class SelectTest {
    @Test
    public void testSelect() {
        assertTableLineage("SELECT col1 FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectWithSchema() {
        assertTableLineage("SELECT col1 FROM schema1.tab1", Set.of("schema1.tab1"));
    }

    @Test
    public void testSelectWithSchemaAndDatabase() {
        assertTableLineage("SELECT col1 FROM db1.schema1.tbl1", Set.of("db1.schema1.tbl1"));
    }

    @Test
    public void testSelectWithTableNameInBacktick() {
        assertTableLineage("SELECT * FROM `tab1`", Set.of("tab1"));
    }

    @Test
    public void testSelectWithSchemaInBacktick() {
        assertTableLineage("SELECT col1 FROM `schema1`.`tab1`", Set.of("schema1.tab1"));
    }

    @Test
    public void testSelectMultiLine() {
        assertTableLineage("SELECT col1 FROM\n" +
                "tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectAsterisk() {
        assertTableLineage("SELECT * FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectValue() {
        assertTableLineage("SELECT 1", Set.of());
    }

    @Test
    public void testSelectFunction() {
        assertTableLineage("SELECT NOW()", Set.of());
    }

    @Test
    public void testSelectTrimFunctionWithFromKeyword() {
        assertTableLineage("SELECT trim(BOTH '  ' FROM '  abc  ')", Set.of());
    }

    @Test
    public void testSelectTrimFunctionWithFromKeywordFromSourceTable() {
        assertTableLineage("SELECT trim(BOTH '  ' FROM col1) FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectWithWhere() {
        assertTableLineage("SELECT * FROM tab1 WHERE col1 > val1 AND col2 = 'val2'", Set.of("tab1"));
    }

    @Test
    public void testSelectWithComment() {
        assertTableLineage("SELECT -- comment1\n col1 FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectWithCommentAfterFrom() {
        assertTableLineage("SELECT col1\nFROM  -- comment\ntab1", Set.of("tab1"));
    }

    @Test
    public void testSelectWithCommentAfterJoin() {
        assertTableLineage("select * from tab1 join --comment\ntab2 on tab1.x = tab2.x", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectKeywordAsColumnAlias() {
        // here `as` is the column alias
        assertTableLineage("SELECT 1 `as` FROM tab1", Set.of("tab1"));
        // the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
        // helper("SELECT 1 as FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectWithTableAlias() {
        assertTableLineage("SELECT 1 FROM tab1 AS alias1", Set.of("tab1"));
    }

    @Test
    public void testSelectCount() {
        assertTableLineage("SELECT COUNT(*) FROM tab1", Set.of("tab1"));
    }

    @Test
    public void testSelectSubquery() {
        assertTableLineage("SELECT col1 FROM (SELECT col1 FROM tab1) dt", Set.of("tab1"));
        // with an extra space
        assertTableLineage("SELECT col1 FROM ( SELECT col1 FROM tab1) dt", Set.of("tab1"));
    }

    @Test
    public void testSelectSubqueryWithTwoParenthesis() {
        assertTableLineage("SELECT col1 FROM ((SELECT col1 FROM tab1)) dt", Set.of("tab1"));
    }

    @Test
    public void testSelectSubqueryWithMoreParenthesis() {
        assertTableLineage("SELECT col1 FROM (((((((SELECT col1 FROM tab1))))))) dt", Set.of("tab1"));
    }

    @Test
    public void testSelectSubqueryInCase() {
        assertTableLineage("SELECT\n" +
                        "CASE WHEN (SELECT count(*) FROM tab1 WHERE col1 = 'tab2') = 1 THEN (SELECT count(*) FROM tab2) ELSE 0 END AS cnt",
                Set.of("tab1", "tab2"));
        assertTableLineage("SELECT\n" +
                        "CASE WHEN 1 = (SELECT count(*) FROM tab1 WHERE col1 = 'tab2') THEN (SELECT count(*) FROM tab2) ELSE 0 END AS cnt",
                Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectSubqueryWithoutAlias() {
        // this syntax is valid in SparkSQL, not for MySQL
        assertTableLineage("SELECT col1 FROM (SELECT col1 FROM tab1)", Set.of("tab1"));
    }

    @Test
    public void testSelectSubqueryInWhereClause() {
        assertTableLineage("SELECT col1\n" +
                "FROM tab1\n" +
                "WHERE col1 IN (SELECT max(col1) FROM tab2)", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectInnerJoin() {
        assertTableLineage("SELECT * FROM tab1 INNER JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectJoin() {
        assertTableLineage("SELECT * FROM tab1 JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectLeftJoin() {
        assertTableLineage("SELECT * FROM tab1 LEFT JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectLeftJoinWithExtraSpaceInMiddle() {
        assertTableLineage("SELECT * FROM tab1 LEFT  JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectLeftSemiJoin() {
        assertTableLineage("SELECT * FROM tab1 LEFT SEMI JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectLeftSemiJoinWithOn() {
        assertTableLineage("SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectRightJoin() {
        assertTableLineage("SELECT * FROM tab1 RIGHT JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectFullOuterJoin() {
        assertTableLineage("SELECT * FROM tab1 FULL OUTER JOIN tab2", Set.of("tab1", "tab2"));
    }

//    @Test
//    public void testSelectFullOuterJoinWithFullAsAlias() {
//        // SparkSQL can't handle this
//         helper("SELECT * FROM tab1 AS full FULL OUTER JOIN tab2", Set.of("tab1", "tab2"));
//    }

    @Test
    public void testSelectCrossJoin() {
        assertTableLineage("SELECT * FROM tab1 CROSS JOIN tab2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectCrossJoinWithOn() {
        assertTableLineage("SELECT * FROM tab1 CROSS JOIN tab2 on tab1.col1 = tab2.col2", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectJoinWithSubquery() {
        assertTableLineage("SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id " +
                "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectJoinInAnsi89Syntax() {
        assertTableLineage("SELECT * FROM tab1 a, tab2 b", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectJoinInAnsi89SyntaxWithSubquery() {
        assertTableLineage("SELECT * FROM (SELECT * FROM tab1) a, (SELECT * FROM tab2) b", Set.of("tab1", "tab2"));
    }

    @Test
    public void testSelectGroupBy() {
        assertTableLineage("SELECT col1, col2 FROM tab1 GROUP BY col1, col2", Set.of("tab1"));
    }

    @Test
    public void testSelectGroupByOrdinal() {
        assertTableLineage("SELECT col1, col2 FROM tab1 GROUP BY 1, 2", Set.of("tab1"));
    }
}
