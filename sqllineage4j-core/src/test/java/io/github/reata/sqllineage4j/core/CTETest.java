package io.github.reata.sqllineage4j.core;

import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertTableLineage;

public class CTETest {
    @Test
    public void testWithSelect() {
        assertTableLineage("WITH tab1 AS (SELECT 1) SELECT * FROM tab1", Set.of());
    }

    @Test
    public void testWithSelectOne() {
        assertTableLineage("WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1", Set.of("schema1.tab1"));
    }

    @Test
    public void testWithSelectOneWithoutAs() {
        // AS in CTE is negligible in SparkSQL, however it is required in MySQL. See below reference
        // https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html
        // https://dev.mysql.com/doc/refman/8.0/en/with.html
        assertTableLineage("WITH wtab1 (SELECT * FROM schema1.tab1) SELECT * FROM wtab1", Set.of("schema1.tab1"));
    }

    @Test
    public void testWithSelectMany() {
        assertTableLineage("WITH\n" +
                "cte1 AS (SELECT a, b FROM table1),\n" +
                "cte2 AS (SELECT c, d FROM table2)\n" +
                "SELECT b, d FROM cte1 JOIN cte2\n" +
                "WHERE cte1.a = cte2.c", Set.of("table1", "table2"));
    }

    @Test
    public void testWithSelectManyReference() {
        assertTableLineage("WITH\n" +
                "cte1 AS (SELECT a, b FROM tab1),\n" +
                "cte2 AS (SELECT a, count(*) AS cnt FROM cte1 GROUP BY a)\n" +
                "SELECT a, b, cnt FROM cte1 JOIN cte2\n" +
                "WHERE cte1.a = cte2.a", Set.of("tab1"));
    }

    @Test
    public void testWithUsingAlias() {
        assertTableLineage("WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1 wt", Set.of("schema1.tab1"));
    }

    @Test
    public void testWithSelectJoinTableWithSameName() {
        assertTableLineage("WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1 CROSS JOIN db.wtab1", Set.of("schema1.tab1", "db.wtab1"));
    }

    @Test
    public void testWithInsert() {
        assertTableLineage("WITH tab1 AS (SELECT * FROM tab2) INSERT INTO tab3 SELECT * FROM tab1", Set.of("tab2"), Set.of("tab3"));
    }

    @Test
    public void testWithInsertOverwrite() {
        assertTableLineage("WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE tab3 SELECT * FROM tab1", Set.of("tab2"), Set.of("tab3"));
    }

    @Test
    public void testWithInsertPlusKeywordTable() {
        assertTableLineage("WITH tab1 AS (SELECT * FROM tab2) INSERT INTO TABLE tab3 SELECT * FROM tab1", Set.of("tab2"), Set.of("tab3"));
    }

    @Test
    public void testWithInsertOverwritePlusKeywordTable() {
        assertTableLineage("WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE TABLE tab3 SELECT * FROM tab1", Set.of("tab2"), Set.of("tab3"));
    }
}
