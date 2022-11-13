package io.github.reata.sqllineage4j.common;

import io.github.reata.sqllineage4j.common.model.Schema;
import io.github.reata.sqllineage4j.common.model.Table;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ModelTest {

    @Test
    public void testDummy() {
        assertNotNull(new Schema().toString());
        assertNotNull(new Table("").toString());
        assertNotNull(new Table("a.b.c").toString());
    }

    @Test
    public void testHashEq() {
        assertEquals(new Schema("a"), new Schema("a"));
        assertEquals(1, new HashSet<>(List.of(new Schema("a"), new Schema("a"))).size());
        assertEquals(new Table("a"), new Table("a"));
        assertEquals(1, new HashSet<>(List.of(new Table("a"), new Table("a"))).size());
    }
}
