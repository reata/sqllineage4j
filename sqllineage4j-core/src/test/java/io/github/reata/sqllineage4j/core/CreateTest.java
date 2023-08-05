package io.github.reata.sqllineage4j.core;

import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertTableLineage;

public class CreateTest {
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
}
