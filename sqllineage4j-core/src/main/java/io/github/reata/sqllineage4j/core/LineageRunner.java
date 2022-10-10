package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.holder.SQLLineageHolder;
import io.github.reata.sqllineage4j.core.holder.StatementLineageHolder;
import io.github.reata.sqllineage4j.parser.LineageParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.stream.Collectors;

public class LineageRunner {
    private final SQLLineageHolder sqlLineageHolder;

    public LineageRunner(String sql) {
        List<ParseTree> stmts = LineageParser.split(sql).stream().map(LineageParser::parse).collect(Collectors.toList());
        sqlLineageHolder = SQLLineageHolder.of(stmts.stream().map(x -> new LineageAnalyzer().analyze(x)).toArray(StatementLineageHolder[]::new));
    }

    public List<Table> sourceTables() {
        return List.copyOf(sqlLineageHolder.getSourceTables());
    }

    public List<Table> targetTables() {
        return List.copyOf(sqlLineageHolder.getTargetTables());
    }
}
