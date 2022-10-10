package io.github.reata.sqllineage4j.parser;

import com.facebook.presto.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.stream.Collectors;

public class LineageParser {

    public static ParseTree parse(String sql) {
        CharStream inputStream = CharStreams.fromString(sql.toUpperCase());
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(inputStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqlBaseLexer);
        SqlBaseParser sqlBaseParser = new SqlBaseParser(commonTokenStream);
        return sqlBaseParser.singleStatement();
    }

    public static List<String> split(String sql) {
        StatementSplitter statementSplitter = new StatementSplitter(sql);
        List<String> stmts = statementSplitter.getCompleteStatements().stream().map(StatementSplitter.Statement::toString).collect(Collectors.toList());
        String partialStatement = statementSplitter.getPartialStatement();
        if (partialStatement.length() > 0) {
            stmts.add(statementSplitter.getPartialStatement());
        }
        return stmts;
    }
}
