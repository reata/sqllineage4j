package io.github.reata.sqllineage4j.core;

import org.junit.Test;

import java.util.Set;

import static io.github.reata.sqllineage4j.core.Helper.assertTableLineage;

public class InsertTest {
    @Test
    public void testInsertInto() {
        assertTableLineage("INSERT INTO tab1 VALUES (1, 2)", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testInsertIntoWithKeywordTable() {
        assertTableLineage("INSERT INTO TABLE tab1 VALUES (1, 2)", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testInsertIntoWithColumns() {
        assertTableLineage("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testInsertIntoWithColumnsAndSelect() {
        assertTableLineage("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testInsertIntoWithColumnsAndSelectUnion() {
        assertTableLineage("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3", Set.of("tab2", "tab3"), Set.of("tab1"));
    }

    @Test
    public void testInsertIntoPartitions() {
        assertTableLineage("INSERT INTO TABLE tab1 PARTITION (par1=1) SELECT * FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testInsertOverwrite() {
        assertTableLineage("INSERT OVERWRITE tab1 SELECT * FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testInsertOverwriteWithKeywordTable() {
        assertTableLineage("INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2", Set.of("tab2"), Set.of("tab1"));
    }

    @Test
    public void testInsertOverwriteValues() {
        assertTableLineage("INSERT OVERWRITE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')", Set.of(), Set.of("tab1"));
    }

    @Test
    public void testInsertOverwriteFromSelf() {
        assertTableLineage("INSERT OVERWRITE TABLE foo\n" +
                "SELECT col from foo\n" +
                "WHERE flag IS NOT NULL", Set.of("foo"), Set.of("foo"));
    }

    @Test
    public void testInsertOverwriteFromSelfWithJoin() {
        assertTableLineage("INSERT OVERWRITE TABLE tab_1\n" +
                "SELECT tab2.col_a from tab_2\n" +
                "JOIN tab_1\n" +
                "ON tab_1.col_a = tab_2.cola", Set.of("tab_1", "tab_2"), Set.of("tab_1"));
    }
}
