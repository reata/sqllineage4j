package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;
import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.holder.StatementLineageHolder;
import io.github.reata.sqllineage4j.parser.SqlBaseBaseListener;
import io.github.reata.sqllineage4j.parser.SqlBaseParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LineageAnalyzer {

    private final StatementLineageHolder statementLineageHolder = new StatementLineageHolder();

    public StatementLineageHolder analyze(ParseTree stmt) {
        ParseTreeWalker walker = new ParseTreeWalker();
        LineageListener listener = new LineageListener();
        walker.walk(listener, stmt);
        listener.getSourceTable().forEach(statementLineageHolder::addRead);
        listener.getIntermediateTable().forEach(statementLineageHolder::addCTE);
        listener.getTargetTable().forEach(statementLineageHolder::addWrite);
        listener.getDrop().forEach(statementLineageHolder::addDrop);
        listener.getRename().forEach(x -> statementLineageHolder.addRename(x.getValue0(), x.getValue1()));
        listener.getColumnLineage().forEach(x -> statementLineageHolder.addColumnLineage(x.getValue0(), x.getValue1()));
        return statementLineageHolder;
    }

    public static class LineageListener extends SqlBaseBaseListener {

        private final Set<String> sourceTable = new HashSet<>();
        private final Set<String> targetTable = new HashSet<>();
        private final Set<String> intermediateTable = new HashSet<>();
        private final Set<String> drop = new HashSet<>();
        private final Set<Pair<String, String>> rename = new HashSet<>();
        private final List<Column> columns = new ArrayList<>();
        private final Set<Pair<Column, Column>> columnLineage = new HashSet<>();

        public Set<String> getSourceTable() {
            return sourceTable;
        }

        public Set<String> getTargetTable() {
            return targetTable;
        }

        public Set<String> getIntermediateTable() {
            return intermediateTable;
        }

        public Set<String> getDrop() {
            return drop;
        }

        public Set<Pair<String, String>> getRename() {
            return rename;
        }

        public Set<Pair<Column, Column>> getColumnLineage() {
            return columnLineage;
        }

        @Override
        public void enterInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
            targetTable.add(ctx.multipartIdentifier().getText());
        }

        @Override
        public void enterInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {
            targetTable.add(ctx.multipartIdentifier().getText());
        }

        @Override
        public void enterCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {
            targetTable.add(ctx.multipartIdentifier().getText());
        }

        @Override
        public void enterCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {
            targetTable.add(ctx.target.getText());
            sourceTable.add(ctx.source.getText());
        }

        @Override
        public void enterUpdateTable(SqlBaseParser.UpdateTableContext ctx) {
            handleMultipartIdentifier(ctx.multipartIdentifier(), "write");
        }

        @Override
        public void enterDropTable(SqlBaseParser.DropTableContext ctx) {
            handleMultipartIdentifier(ctx.multipartIdentifier(), "drop");
        }

        @Override
        public void enterRenameTable(SqlBaseParser.RenameTableContext ctx) {
            rename.add(Pair.with(ctx.from.getText(), ctx.to.getText()));
        }

        @Override
        public void enterFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx) {
            SqlBaseParser.UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommandsContext = ctx.unsupportedHiveNativeCommands();
            if (unsupportedHiveNativeCommandsContext != null) {
                if (unsupportedHiveNativeCommandsContext.ALTER() != null
                        && unsupportedHiveNativeCommandsContext.TABLE() != null
                        && unsupportedHiveNativeCommandsContext.EXCHANGE() != null
                        && unsupportedHiveNativeCommandsContext.PARTITION() != null) {
                    targetTable.add(unsupportedHiveNativeCommandsContext.tableIdentifier().getText());
                    sourceTable.add(ctx.getChild(ctx.getChildCount() - 1).getText());
                }
            }
        }

        @Override
        public void enterDmlStatement(SqlBaseParser.DmlStatementContext ctx) {
            SqlBaseParser.CtesContext withContext = ctx.ctes();
            if (withContext != null) {
                for (SqlBaseParser.NamedQueryContext namedQueryContext : withContext.namedQuery()) {
                    intermediateTable.add(namedQueryContext.errorCapturingIdentifier().getText());
                }
            }
        }

        @Override
        public void enterQuery(SqlBaseParser.QueryContext ctx) {
            SqlBaseParser.CtesContext withContext = ctx.ctes();
            if (withContext != null) {
                for (SqlBaseParser.NamedQueryContext namedQueryContext : withContext.namedQuery()) {
                    intermediateTable.add(namedQueryContext.errorCapturingIdentifier().getText());
                }
            }
        }

        @Override
        public void enterRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
            SqlBaseParser.FromClauseContext fromClauseContext = ctx.fromClause();
            if (fromClauseContext != null) {
                for (SqlBaseParser.RelationContext relationContext : ctx.fromClause().relation()) {
                    handleRelationPrimary(relationContext.relationPrimary());
                    for (SqlBaseParser.JoinRelationContext joinRelationContext : relationContext.joinRelation()) {
                        handleRelationPrimary(joinRelationContext.relationPrimary());
                    }
                }
            }
        }

        @Override
        public void exitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
            String tgtTbl = "";
            if (getTargetTable().size() == 1) {
                tgtTbl = List.copyOf(getTargetTable()).get(0);
            }
            if (!tgtTbl.equals("")) {
                for (Column tgtCol : columns) {
                    tgtCol.setParent(new Table(tgtTbl));
                    for (Column srcCol : tgtCol.toSourceColumns(getAliasMappingFromTableGroup())) {
                        columnLineage.add(Pair.with(srcCol, tgtCol));
                    }
                }
            }
        }

        @Override
        public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
            for (SqlBaseParser.NamedExpressionContext namedExpressionContext : ctx.namedExpressionSeq().namedExpression()) {
                String alias = getIdentiferName(namedExpressionContext.errorCapturingIdentifier());
                SqlBaseParser.BooleanExpressionContext booleanExpressionContext = namedExpressionContext.expression().booleanExpression();
                if (booleanExpressionContext instanceof SqlBaseParser.PredicatedContext) {
                    SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) booleanExpressionContext;
                    SqlBaseParser.ValueExpressionContext valueExpressionContext = predicatedContext.valueExpression();
                    if (valueExpressionContext instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                        SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext = (SqlBaseParser.ValueExpressionDefaultContext) valueExpressionContext;
                        SqlBaseParser.PrimaryExpressionContext primaryExpressionContext = valueExpressionDefaultContext.primaryExpression();
                        if (primaryExpressionContext instanceof SqlBaseParser.ColumnReferenceContext) {
                            SqlBaseParser.ColumnReferenceContext columnReferenceContext = (SqlBaseParser.ColumnReferenceContext) primaryExpressionContext;
                            String columnName = columnReferenceContext.getText();
                            Column column = new Column(alias.equals("") ? columnName : alias);
                            column.setSourceColumns(ColumnQualifierTuple.create(columnName, null));
                            columns.add(column);
                        } else if (primaryExpressionContext instanceof SqlBaseParser.DereferenceContext) {
                            SqlBaseParser.DereferenceContext dereferenceContext = (SqlBaseParser.DereferenceContext) primaryExpressionContext;
                            String columnName = dereferenceContext.identifier().strictIdentifier().getText();
                            Column column = new Column(alias.equals("") ? columnName : alias);
                            column.setSourceColumns(ColumnQualifierTuple.create(columnName, null));
                            columns.add(column);
                        }
                    }
                }
            }
        }

        @Override
        public void enterFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
            if (ctx.functionName().getText().equalsIgnoreCase("swap_partitions_between_tables")) {
                List<SqlBaseParser.ExpressionContext> arguments = ctx.argument;
                if (arguments.size() == 4) {
                    sourceTable.add(arguments.get(0).getText().replace("'", "").replace("\"", ""));
                    targetTable.add(arguments.get(3).getText().replace("'", "").replace("\"", ""));
                }
            }
        }

        private void handleRelationPrimary(SqlBaseParser.RelationPrimaryContext relationPrimaryContext) {
            if (relationPrimaryContext instanceof SqlBaseParser.TableNameContext) {
                SqlBaseParser.TableNameContext tableNameContext = (SqlBaseParser.TableNameContext) relationPrimaryContext;
                handleMultipartIdentifier(tableNameContext.multipartIdentifier(), "read");
            } else if (relationPrimaryContext instanceof SqlBaseParser.AliasedRelationContext) {
                SqlBaseParser.AliasedRelationContext aliasedRelationContext = (SqlBaseParser.AliasedRelationContext) relationPrimaryContext;
                handleRelationPrimary(aliasedRelationContext.relation().relationPrimary());
            }
        }

        private void handleMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext, String type) {
            List<String> unquotedParts = new ArrayList<>();
            for (SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext : multipartIdentifierContext.errorCapturingIdentifier()) {
                String identifier = getIdentiferName(errorCapturingIdentifierContext);
                if (!identifier.equals("")) {
                    unquotedParts.add(identifier);
                }
            }
            switch (type) {
                case "read":
                    sourceTable.add(String.join(".", unquotedParts));
                    break;
                case "write":
                    targetTable.add(String.join(".", unquotedParts));
                    break;
                case "drop":
                    drop.add(String.join(".", unquotedParts));
                    break;
            }
        }

        private String getIdentiferName(SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext) {
            String name = "";
            if (errorCapturingIdentifierContext != null) {
                SqlBaseParser.StrictIdentifierContext strictIdentifierContext = errorCapturingIdentifierContext.identifier().strictIdentifier();
                if (strictIdentifierContext instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
                    name = strictIdentifierContext.getText().replace("`", "");
                } else if (strictIdentifierContext instanceof SqlBaseParser.UnquotedIdentifierContext) {
                    name = strictIdentifierContext.getText();
                }
            }
            return name;
        }

        private Map<String, Table> getAliasMappingFromTableGroup() {
            return sourceTable.stream().collect(Collectors.toMap(Function.identity(), Table::new));
        }
    }
}
