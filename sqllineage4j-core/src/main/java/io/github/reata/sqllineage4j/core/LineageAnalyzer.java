package io.github.reata.sqllineage4j.core;

import io.github.reata.sqllineage4j.common.constant.NodeTag;
import io.github.reata.sqllineage4j.common.entity.ColumnQualifierTuple;
import io.github.reata.sqllineage4j.common.model.Column;
import io.github.reata.sqllineage4j.common.model.QuerySet;
import io.github.reata.sqllineage4j.common.model.SubQuery;
import io.github.reata.sqllineage4j.common.model.Table;
import io.github.reata.sqllineage4j.core.holder.StatementLineageHolder;
import io.github.reata.sqllineage4j.core.holder.SubQueryLineageHolder;
import io.github.reata.sqllineage4j.parser.SqlBaseBaseListener;
import io.github.reata.sqllineage4j.parser.SqlBaseParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LineageAnalyzer {

    public StatementLineageHolder analyze(ParseTree stmt) {
        ParseTreeWalker walker = new ParseTreeWalker();
        LineageListener listener = new LineageListener();
        walker.walk(listener, stmt);
        return listener.getStatementLineageHolder();
    }

    public static class LineageListener extends SqlBaseBaseListener {

        private final StatementLineageHolder statementLineageHolder = new StatementLineageHolder();
        private final Map<Integer, SubQueryLineageHolder> subQueryLineageHolders = new HashMap<>();

        public StatementLineageHolder getStatementLineageHolder() {
            return statementLineageHolder;
        }

        private String getOriginalText(ParserRuleContext parserRuleContext) {
            CharStream stream = parserRuleContext.start.getInputStream();
            return stream.getText(new Interval(parserRuleContext.start.getStartIndex(), parserRuleContext.stop.getStopIndex()));
        }

        private SubQueryLineageHolder getHolder(ParserRuleContext ctx) {
            while (ctx.getParent() != null) {
                ctx = ctx.getParent();
                if (ctx instanceof SqlBaseParser.RegularQuerySpecificationContext) {
                    return subQueryLineageHolders.get(ctx.hashCode());
                }
            }
            return null;
        }

        @Override
        public void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
            for (SubQueryLineageHolder holder : subQueryLineageHolders.values()) {
                statementLineageHolder.union(holder);
            }
        }

        @Override
        public void enterInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
            statementLineageHolder.addWrite(new Table(ctx.multipartIdentifier().getText()));
        }

        @Override
        public void enterInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {
            statementLineageHolder.addWrite(new Table(ctx.multipartIdentifier().getText()));
        }

        @Override
        public void enterCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {
            statementLineageHolder.addWrite(new Table(ctx.multipartIdentifier().getText()));
        }

        @Override
        public void enterCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {
            statementLineageHolder.addWrite(new Table(ctx.target.getText()));
            statementLineageHolder.addRead(new Table(ctx.source.getText()));
        }

        @Override
        public void enterUpdateTable(SqlBaseParser.UpdateTableContext ctx) {
            handleMultipartIdentifier(ctx.multipartIdentifier(), NodeTag.WRITE, null);
        }

        @Override
        public void enterDropTable(SqlBaseParser.DropTableContext ctx) {
            handleMultipartIdentifier(ctx.multipartIdentifier(), NodeTag.DROP, null);
        }

        @Override
        public void enterRenameTable(SqlBaseParser.RenameTableContext ctx) {
            statementLineageHolder.addRename(new Table(ctx.from.getText()), new Table(ctx.to.getText()));
        }

        @Override
        public void enterFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx) {
            SqlBaseParser.UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommandsContext = ctx.unsupportedHiveNativeCommands();
            if (unsupportedHiveNativeCommandsContext != null) {
                if (unsupportedHiveNativeCommandsContext.ALTER() != null
                        && unsupportedHiveNativeCommandsContext.TABLE() != null
                        && unsupportedHiveNativeCommandsContext.EXCHANGE() != null
                        && unsupportedHiveNativeCommandsContext.PARTITION() != null) {
                    statementLineageHolder.addWrite(new Table(unsupportedHiveNativeCommandsContext.tableIdentifier().getText()));
                    statementLineageHolder.addRead(new Table(ctx.getChild(ctx.getChildCount() - 1).getText()));
                }
            }
        }

        @Override
        public void enterCtes(SqlBaseParser.CtesContext ctx) {
            for (SqlBaseParser.NamedQueryContext namedQueryContext : ctx.namedQuery()) {
                if (namedQueryContext.query() != null) {
                    statementLineageHolder.addCTE(new SubQuery(
                            namedQueryContext.query().getText(),
                            namedQueryContext.errorCapturingIdentifier().getText()
                    ));
                }
            }
        }

        @Override
        public void enterRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
            SubQueryLineageHolder holder = new SubQueryLineageHolder();
            subQueryLineageHolders.put(ctx.hashCode(), holder);
            ParserRuleContext parentCtx = ctx;
            boolean isSubQuery = false;
            while (parentCtx.getParent() != null) {
                parentCtx = parentCtx.getParent();
                if (parentCtx instanceof SqlBaseParser.AliasedQueryContext) {
                    isSubQuery = true;
                    SqlBaseParser.AliasedQueryContext aliasedQueryContext = (SqlBaseParser.AliasedQueryContext) parentCtx;
                    SubQuery subQuery = new SubQuery(aliasedQueryContext.query().getText(), aliasedQueryContext.tableAlias().getText());
                    holder.addWrite(subQuery);
                    break;
                } else if (parentCtx instanceof SqlBaseParser.NamedQueryContext) {
                    isSubQuery = true;
                    SqlBaseParser.NamedQueryContext namedQueryContext = (SqlBaseParser.NamedQueryContext) parentCtx;
                    SubQuery subQuery = new SubQuery(namedQueryContext.query().getText(), namedQueryContext.errorCapturingIdentifier().getText());
                    holder.addWrite(subQuery);
                    break;
                }
            }
            if (!isSubQuery) {
                if (statementLineageHolder.getWrite().size() > 0) {
                    holder.addWrite(new ArrayList<>(statementLineageHolder.getWrite()).get(0));
                }
            }
        }

        @Override
        public void exitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
            SubQueryLineageHolder holder = subQueryLineageHolders.get(ctx.hashCode());
            QuerySet tgtTbl = null;
            if (holder.getWrite().size() == 1) {
                tgtTbl = List.copyOf(holder.getWrite()).get(0);
            }
            if (tgtTbl != null) {
                for (Column tgtCol : holder.getSelectColumns()) {
                    tgtCol.setParent(tgtTbl);
                    Map<String, QuerySet> aliasMapping = getAliasMappingFromTableGroup(holder);
                    for (Column srcCol : tgtCol.toSourceColumns(aliasMapping)) {
                        holder.addColumnLineage(srcCol, tgtCol);
                    }
                }
            }
        }

        @Override
        public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
            for (SqlBaseParser.NamedExpressionContext namedExpressionContext : ctx.namedExpressionSeq().namedExpression()) {
                String alias = getIdentifierName(namedExpressionContext.errorCapturingIdentifier());
                SqlBaseParser.BooleanExpressionContext booleanExpressionContext = namedExpressionContext.expression().booleanExpression();
                handleBooleanExpression(booleanExpressionContext, alias);
            }
        }

        @Override
        public void enterFromClause(SqlBaseParser.FromClauseContext ctx) {
            for (SqlBaseParser.RelationContext relationContext : ctx.relation()) {
                handleRelationPrimary(relationContext.relationPrimary());
                for (SqlBaseParser.JoinRelationContext joinRelationContext : relationContext.joinRelation()) {
                    handleRelationPrimary(joinRelationContext.relationPrimary());
                }
            }
        }

        @Override
        public void enterFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
            if (ctx.functionName().getText().equalsIgnoreCase("swap_partitions_between_tables")) {
                List<SqlBaseParser.ExpressionContext> arguments = ctx.argument;
                if (arguments.size() == 4) {
                    statementLineageHolder.addRead(new Table(arguments.get(0).getText().replace("'", "").replace("\"", "")));
                    statementLineageHolder.addWrite(new Table(arguments.get(3).getText().replace("'", "").replace("\"", "")));
                }
            }
        }

        private void handleRelationPrimary(SqlBaseParser.RelationPrimaryContext relationPrimaryContext) {
            if (relationPrimaryContext instanceof SqlBaseParser.TableNameContext) {
                SqlBaseParser.TableNameContext tableNameContext = (SqlBaseParser.TableNameContext) relationPrimaryContext;
                String alias = null;
                if (tableNameContext.tableAlias().strictIdentifier() != null) {
                    alias = getOriginalText(tableNameContext.tableAlias().strictIdentifier());
                }
                handleMultipartIdentifier(tableNameContext.multipartIdentifier(), NodeTag.READ, alias);
            } else if (relationPrimaryContext instanceof SqlBaseParser.AliasedRelationContext) {
                SqlBaseParser.AliasedRelationContext aliasedRelationContext = (SqlBaseParser.AliasedRelationContext) relationPrimaryContext;
                handleRelationPrimary(aliasedRelationContext.relation().relationPrimary());
            } else if (relationPrimaryContext instanceof SqlBaseParser.AliasedQueryContext) {
                SqlBaseParser.AliasedQueryContext aliasedQueryContext = (SqlBaseParser.AliasedQueryContext) relationPrimaryContext;
                SubQueryLineageHolder holder = getHolder(relationPrimaryContext);
                Objects.requireNonNull(holder).addRead(new SubQuery(aliasedQueryContext.query().getText(), aliasedQueryContext.tableAlias().getText()));
            }
        }

        private void handleMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext, String type, String alias) {
            SubQueryLineageHolder holder = getHolder(multipartIdentifierContext);
            List<String> unquotedParts = new ArrayList<>();
            for (SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext : multipartIdentifierContext.errorCapturingIdentifier()) {
                String identifier = getIdentifierName(errorCapturingIdentifierContext);
                if (!identifier.equals("")) {
                    unquotedParts.add(identifier);
                }
            }
            String rawName = String.join(".", unquotedParts);
            Table table = alias == null ? new Table(rawName) : new Table(rawName, alias);
            switch (type) {
                case NodeTag.READ:
                    Map<String, SubQuery> cteMap = statementLineageHolder.getCTE().stream().collect(Collectors.toMap(SubQuery::getAlias, Function.identity()));
                    if (cteMap.containsKey(rawName)) {
                        SubQuery cte = cteMap.get(rawName);
                        if (alias != null) {
                            Objects.requireNonNull(holder).addRead(new SubQuery(cte.getQuery(), alias));
                        }
                        Objects.requireNonNull(holder).addRead(cte);
                    } else {
                        Objects.requireNonNull(holder).addRead(table);
                    }
                    break;
                case NodeTag.WRITE:
                    Objects.requireNonNullElse(holder, statementLineageHolder).addWrite(table);
                    break;
                case NodeTag.DROP:
                    statementLineageHolder.addDrop(table);
                    break;
            }
        }

        private void handleBooleanExpression(SqlBaseParser.BooleanExpressionContext booleanExpressionContext, String alias) {
            if (booleanExpressionContext instanceof SqlBaseParser.PredicatedContext) {
                SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) booleanExpressionContext;
                SqlBaseParser.ValueExpressionContext valueExpressionContext = predicatedContext.valueExpression();
                handleValueExpression(valueExpressionContext, alias);
            } else if (booleanExpressionContext instanceof SqlBaseParser.LogicalBinaryContext) {
                SqlBaseParser.LogicalBinaryContext logicalBinaryContext = (SqlBaseParser.LogicalBinaryContext) booleanExpressionContext;
                for (SqlBaseParser.BooleanExpressionContext subBooleanExpressionContext : logicalBinaryContext.booleanExpression()) {
                    handleBooleanExpression(subBooleanExpressionContext, alias);
                }
            }
        }

        private void handleValueExpression(SqlBaseParser.ValueExpressionContext valueExpressionContext, String alias) {
            SubQueryLineageHolder holder = getHolder(valueExpressionContext);
            List<Column> selectColumns = Objects.requireNonNull(holder).getSelectColumns();
            if (valueExpressionContext instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext = (SqlBaseParser.ValueExpressionDefaultContext) valueExpressionContext;
                SqlBaseParser.PrimaryExpressionContext primaryExpressionContext = valueExpressionDefaultContext.primaryExpression();
                if (primaryExpressionContext instanceof SqlBaseParser.ColumnReferenceContext) {
                    SqlBaseParser.ColumnReferenceContext columnReferenceContext = (SqlBaseParser.ColumnReferenceContext) primaryExpressionContext;
                    String columnName = columnReferenceContext.getText();
                    Column column = new Column(alias.equals("") ? columnName : alias);
                    column.setSourceColumns(ColumnQualifierTuple.create(columnName, null));
                    selectColumns.add(column);
                } else if (primaryExpressionContext instanceof SqlBaseParser.DereferenceContext) {
                    SqlBaseParser.DereferenceContext dereferenceContext = (SqlBaseParser.DereferenceContext) primaryExpressionContext;
                    String columnName = dereferenceContext.identifier().strictIdentifier().getText();
                    Column column = new Column(alias.equals("") ? columnName : alias);
                    String qualifierName = dereferenceContext.primaryExpression().getText();
                    column.setSourceColumns(ColumnQualifierTuple.create(columnName, qualifierName));
                    selectColumns.add(column);
                } else if (primaryExpressionContext instanceof SqlBaseParser.StarContext) {
                    SqlBaseParser.StarContext starContext = (SqlBaseParser.StarContext) primaryExpressionContext;
                    String columnName = starContext.ASTERISK().getText();
                    Column column = new Column(alias.equals("") ? columnName : alias);
                    column.setSourceColumns(ColumnQualifierTuple.create(columnName, null));
                    selectColumns.add(column);
                } else if (primaryExpressionContext instanceof SqlBaseParser.FunctionCallContext) {
                    SqlBaseParser.FunctionCallContext functionCallContext = (SqlBaseParser.FunctionCallContext) primaryExpressionContext;
                    for (SqlBaseParser.ExpressionContext expressionContext : functionCallContext.expression()) {
                        handleBooleanExpression(expressionContext.booleanExpression(), alias.equals("") ? functionCallContext.getText() : alias);
                    }
                    if (functionCallContext.windowSpec() != null) {
                        SqlBaseParser.WindowSpecContext windowSpecContext = functionCallContext.windowSpec();
                        if (windowSpecContext instanceof SqlBaseParser.WindowDefContext) {
                            SqlBaseParser.WindowDefContext windowDefContext = (SqlBaseParser.WindowDefContext) windowSpecContext;
                            for (SqlBaseParser.ExpressionContext expressionContext : windowDefContext.expression()) {
                                handleBooleanExpression(expressionContext.booleanExpression(), alias.equals("") ? functionCallContext.getText() : alias);
                            }
                            for (SqlBaseParser.SortItemContext sortItemContext : windowDefContext.sortItem()) {
                                handleBooleanExpression(sortItemContext.expression().booleanExpression(), alias.equals("") ? functionCallContext.getText() : alias);
                            }
                        }
                    }
                } else if (primaryExpressionContext instanceof SqlBaseParser.CastContext) {
                    SqlBaseParser.CastContext castContext = (SqlBaseParser.CastContext) primaryExpressionContext;
                    handleBooleanExpression(castContext.expression().booleanExpression(), alias.equals("") ? getOriginalText(castContext) : alias);
                } else if (primaryExpressionContext instanceof SqlBaseParser.ParenthesizedExpressionContext) {
                    SqlBaseParser.ParenthesizedExpressionContext parenthesizedExpressionContext = (SqlBaseParser.ParenthesizedExpressionContext) primaryExpressionContext;
                    handleBooleanExpression(parenthesizedExpressionContext.expression().booleanExpression(), alias);
                } else if (primaryExpressionContext instanceof SqlBaseParser.SearchedCaseContext) {
                    SqlBaseParser.SearchedCaseContext searchedCaseContext = (SqlBaseParser.SearchedCaseContext) primaryExpressionContext;
                    alias = alias.equals("") ? getOriginalText(searchedCaseContext) : alias;
                    for (SqlBaseParser.WhenClauseContext whenClauseContext : searchedCaseContext.whenClause()) {
                        for (SqlBaseParser.ExpressionContext expressionContext : whenClauseContext.expression()) {
                            handleBooleanExpression(expressionContext.booleanExpression(), alias);
                        }
                    }
                    if (searchedCaseContext.expression() != null) {
                        handleBooleanExpression(searchedCaseContext.expression().booleanExpression(), alias);
                    }
                }
            } else if (valueExpressionContext instanceof SqlBaseParser.ComparisonContext) {
                SqlBaseParser.ComparisonContext comparisonContext = (SqlBaseParser.ComparisonContext) valueExpressionContext;
                for (SqlBaseParser.ValueExpressionContext subValueExpressionContext : comparisonContext.valueExpression()) {
                    handleValueExpression(subValueExpressionContext, alias);
                }
            } else if (valueExpressionContext instanceof SqlBaseParser.ArithmeticBinaryContext) {
                SqlBaseParser.ArithmeticBinaryContext arithmeticBinaryContext = (SqlBaseParser.ArithmeticBinaryContext) valueExpressionContext;
                alias = alias.equals("") ? getOriginalText(arithmeticBinaryContext) : alias;
                for (SqlBaseParser.ValueExpressionContext subValueExpressionContext : arithmeticBinaryContext.valueExpression()) {
                    handleValueExpression(subValueExpressionContext, alias);
                }
            }
        }

        private String getIdentifierName(SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext) {
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

        private Map<String, QuerySet> getAliasMappingFromTableGroup(SubQueryLineageHolder holder) {
            Map<String, QuerySet> alias = holder.getQuerySetAlias();
            for (QuerySet dataset : holder.getRead()) {
                alias.put(dataset.toString(), dataset);
                // TODO: rawName -> dataset
            }
            return alias;
        }
    }
}
