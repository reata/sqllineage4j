package io.github.reata.sqllineage4j.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class LineageParser {

    public static ParseTree parse(String sql) {
        CharStream inputStream = CharStreams.fromString(sql.toUpperCase());
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(inputStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqlBaseLexer);
        SqlBaseParser sqlBaseParser = new SqlBaseParser(commonTokenStream);
        return sqlBaseParser.singleStatement();
    }
}
