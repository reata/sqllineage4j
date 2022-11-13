package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.holder.SQLLineageHolder;
import io.github.reata.sqllineage4j.core.holder.StatementLineageHolder;
import io.github.reata.sqllineage4j.parser.LineageParser;

import java.util.List;
import java.util.stream.Collectors;

public class LineageRunner {

    public static final class Builder {
        private final String sql;
        private boolean verbose = false;

        private Builder (final String sql) {
            this.sql = sql;
        }

        public Builder verbose() {
            this.verbose = true;
            return this;
        }

        public LineageRunner build() {
            if (sql == null) {
                throw new IllegalArgumentException("sql string must be specified");
            }
            return new LineageRunner(this);
        }
    }


    private final List<StatementLineageHolder> statementLineageHolders;
    private final SQLLineageHolder sqlLineageHolder;
    private final List<String> statements;

    private final boolean verbose;

    private LineageRunner(final Builder builder) {
        String sql = builder.sql;
        this.verbose = builder.verbose;
        statements = LineageParser.split(sql);
        statementLineageHolders = statements.stream().map(x -> new LineageAnalyzer().analyze(LineageParser.parse(x))).collect(Collectors.toList());
        sqlLineageHolder = SQLLineageHolder.of(statementLineageHolders.toArray(StatementLineageHolder[]::new));
    }

    public List<Table> sourceTables() {
        return List.copyOf(sqlLineageHolder.getSourceTables());
    }

    public List<Table> targetTables() {
        return List.copyOf(sqlLineageHolder.getTargetTables());
    }

    public List<Table> intermediateTables() {
        return List.copyOf(sqlLineageHolder.getIntermediateTables());
    }

    public void printTableLineage() {
        String sourceTables = sourceTables().stream().map(t -> "    " + t.toString() + "\n").collect(Collectors.joining());
        String targetTables = targetTables().stream().map(t -> "    " + t.toString() + "\n").collect(Collectors.joining());
        String combined = "Statements(#): " + statements.size() + "\n"
                + "Source Tables:\n"
                + sourceTables
                + "Target Tables:\n"
                + targetTables;
        if (intermediateTables().size() > 0) {
            String intermediateTables = intermediateTables().stream().map(t -> "    " + t.toString() + "\n").collect(Collectors.joining());
            combined += "Intermediate Tables:\n" + intermediateTables;
        }
        if (verbose) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < statementLineageHolders.size(); i++) {
                String stmtShort = statements.get(i).replace("\n", "");
                if (stmtShort.length() > 50) {
                    stmtShort = stmtShort.substring(0, 50) + "...";
                }
                String content = statementLineageHolders.get(i).toString().replace("\n", "\n    ");
                result.append("Statement #").append(i + 1).append(": ").append(stmtShort).append("\n    ").append(content).append("\n");
            }
            combined = result + "==========\nSummary:\n" + combined;
        }
        System.out.println(combined);
    }

    public static Builder builder(final String sql) {
        return new Builder(sql);
    }
}
