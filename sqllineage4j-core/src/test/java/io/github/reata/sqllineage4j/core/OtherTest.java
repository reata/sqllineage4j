package io.github.reata.sqllineage4j.core;

import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertTableLineage;

public class OtherTest {
    @Test
    public void testUse() {
        assertTableLineage("USE db1", Set.of());
    }

    @Test
    public void testTableNameCase() {
        assertTableLineage("insert overwrite table tab_a\n" +
                "select * from tab_b\n" +
                "union all\n" +
                "select * from TAB_B", Set.of("tab_b"), Set.of("tab_a"));
    }

    @Test
    public void testCreate() {
        assertTableLineage("CREATE TABLE tab1 (col1 STRING)", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testCreateIfNotExist() {
        assertTableLineage("CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testCreateBucketTable() {
        assertTableLineage("CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testCreateAs() {
        assertTableLineage("CREATE TABLE tab1 AS SELECT * FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateAsWithParenthesisAroundSelectStatement() {
        assertTableLineage("CREATE TABLE tab1 AS (SELECT * FROM tab2)", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateAsWithParenthesisAroundTableName() {
        assertTableLineage("CREATE TABLE tab1 AS SELECT * FROM (tab2)", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateAsWithParenthesisAroundBoth() {
        assertTableLineage("CREATE TABLE tab1 AS (SELECT * FROM (tab2))", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateLike() {
        assertTableLineage("CREATE TABLE tab1 LIKE tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateSelect() {
        assertTableLineage("CREATE TABLE tab1 SELECT * FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testCreateAfterDrop() {
        // multiple statements SQL not supported yet, come back later
        assertTableLineage("DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testCreateUsingSerde() {
        // Check https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
        // here with is not an indicator for CTE
        assertTableLineage("CREATE TABLE apachelog (\n" +
                "  host STRING,\n" +
                "  identity STRING,\n" +
                "  user STRING,\n" +
                "  time STRING,\n" +
                "  request STRING,\n" +
                "  status STRING,\n" +
                "  size STRING,\n" +
                "  referer STRING,\n" +
                "  agent STRING)\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "  \"input.regex\" = \"([^]*) ([^]*) ([^]*) (-|\\\\[^\\\\]*\\\\]) ([^ \\\"]*|\\\"[^\\\"]*\\\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \\\"]*|\\\".*\\\") ([^ \\\"]*|\\\".*\\\"))?\"\n" +
                ")\n" +
                "STORED AS TEXTFILE", Set.of(), Set.of("apachelog"));
    }

    @Test
    public void testBucketWithUsingParenthesis() {
        assertTableLineage("CREATE TABLE tbl1 (col1 VARCHAR)\n" +
                "WITH (bucketed_on = array['col1'], bucket_count = 256);", Set.of(), Set.of("tbl1"));
    }

    @Test
    public void testUpdate() {
        assertTableLineage("UPDATE tab1 SET col1='val1' WHERE col2='val2'", Set.of(), Set.of("tab1"));
    }

//    @Test
//    public void testUpdateWithJoin() {
//        // SparkSQL doesn't support this syntax
//        helper("UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2", Set.of("tab2"), Set.of("tab1"));
//    }

//    @Test
//    public void testCopyFromTable() {
//        // SparkSQL doesn't support this syntax
//        helper("COPY tab1 FROM tab2", Set.of("tab2"), Set.of("tab1"));
//    }

    @Test
    public void testDrop() {
        assertTableLineage("DROP TABLE IF EXISTS tab1", Set.of(), Set.of());
    }

    @Test
    public void testDropWithComment() {
        assertTableLineage("--comment\n" +
                "DROP TABLE IF EXISTS tab1", Set.of(), Set.of());
    }

//    @Test
//    public void testDropAfterCreate() {
//        // multiple statements SQL not supported yet, come back later
//        assertTableLineage("CREATE TABLE IF NOT EXISTS tab1 (col1 STRING);DROP TABLE IF EXISTS tab1", Set.of(), Set.of("tab1"));
//    }
//
//    @Test
//    public void testDropTmpTabAfterCreate() {
//        // multiple statements SQL not supported yet, come back later
//        helper("create table tab_a as select * from tab_b;\n" +
//                "insert overwrite table tab_c select * from tab_a;\n" +
//                "drop table tab_a", Set.of("tab_b"), Set.of("tab_c"));
//    }
//
//    @Test
//    public void testNewCreateTabAsTmpTable() {
//        // multiple statements SQL not supported yet, come back later
//        helper("create table tab_a as select * from tab_b;\n" +
//                "create table tab_c as select * from tab_a;", Set.of("tab_b"), Set.of("tab_c"));
//    }

    @Test
    public void testAlterTableRename() {
        assertTableLineage("alter table tab1 rename to tab2", Set.of(), Set.of());
    }

//    /*
//     This syntax is MySQL specific:
//     https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
//     */
//    @Test
//    public void testRenameTable() {
//        // SparkSQL doesn't support this syntax
//        helper("rename table tab1 to tab2", Set.of(), Set.of());
//    }
//
//    @Test
//    public void testRenameTables() {
//        // SparkSQL doesn't support this syntax
//        helper("rename table tab1 to tab2, tab3 to tab4", Set.of(), Set.of());
//    }

    /*
     See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
     */
    @Test
    public void testAlterTableExchangePartition() {
        assertTableLineage("alter table tab1 exchange partition(pt='part1') with table tab2", Set.of("tab2"), Set.of("tab1"));
    }

    /*
     See https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/AdministratorsGuide/Partitions/SwappingPartitions.htm
     for language specification
     */
    @Test
    public void testSwappingPartitions() {
        assertTableLineage("select swap_partitions_between_tables('staging', 'min-range-value', 'max-range-value', 'target')", Set.of("staging"), Set.of("target"));
    }

//    @Test
//    public void testAlterTargetTableName() {
//        // multiple statements SQL not supported yet, come back later
//        helper("insert overwrite tab1 select * from tab2; alter table tab1 rename to tab3;", Set.of("tab2"), Set.of("tab3"));
//    }

    @Test
    public void testRefreshTable() {
        assertTableLineage("refresh table tab1", Set.of(), Set.of());
    }

    @Test
    public void testCacheTable() {
        assertTableLineage("cache table tab1", Set.of(), Set.of());
    }

    @Test
    public void testUncacheTable() {
        assertTableLineage("uncache table tab1", Set.of(), Set.of());
    }

    @Test
    public void testUncacheTableIfExists() {
        assertTableLineage("uncache table if exists tab1", Set.of(), Set.of());
    }

    @Test
    public void testTruncateTable() {
        assertTableLineage("truncate table tab1", Set.of(), Set.of());
    }

    @Test
    public void testDeleteFromTable() {
        assertTableLineage("delete from table tab1", Set.of(), Set.of());
    }

    @Test
    public void testLateralViewUsingJsonTuple() {
        assertTableLineage("INSERT OVERWRITE TABLE foo\n" +
                "SELECT sc.id, q.item0, q.item1\n" +
                "FROM bar sc\n" +
                "LATERAL VIEW json_tuple(sc.json, 'key1', 'key2') q AS item0, item1", Set.of("bar"), Set.of("foo"));
    }

    @Test
    public void testLateralViewOuter() {
        assertTableLineage("INSERT OVERWRITE TABLE foo\n" +
                "SELECT sc.id, q.col1\n" +
                "FROM bar sc\n" +
                "LATERAL VIEW OUTER explode(sc.json_array) q AS col1", Set.of("bar"), Set.of("foo"));
    }

    @Test
    public void testShowCreateTable() {
        assertTableLineage("show create table tab1", Set.of());
    }
}
