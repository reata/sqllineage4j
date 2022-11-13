package io.github.reata.sqllineage4j.cli;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SQLLineage4jTest {

    @Test
    public void testCliDummy() {
        String testSql = "insert overwrite table foo select * from dual inner join laud; insert overwrite table bar select * from foo";
        SQLLineage4j.main(new String[]{});
        SQLLineage4j.main(new String[]{"-e", testSql});
        SQLLineage4j.main(new String[]{"-e", testSql, "-v"});
        try {
            File f = File.createTempFile("test", ".sql");
            FileWriter fw = new FileWriter(f);
            fw.write(testSql);
            fw.close();
            SQLLineage4j.main(new String[]{"-f", f.getAbsolutePath()});
            SQLLineage4j.main(new String[]{"-e", testSql,  "-f", f.getAbsolutePath()});
            assertTrue(f.delete());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileException() throws Exception {
        int statusCode = SystemLambda.catchSystemExit(() -> SQLLineage4j.main(new String[]{"-f", "nonexist_file"}));
        assertEquals(1, statusCode);
    }

    @Test
    public void testFilePermissionError() throws Exception {
        int statusCode = SystemLambda.catchSystemExit(() -> SQLLineage4j.main(new String[]{"-f", "/"}));
        assertEquals(1, statusCode);
    }
}
