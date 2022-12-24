package io.github.reata.sqllineage4j.parser;

import java.util.ArrayList;
import java.util.List;

/*
 This is from Spark's SparkSQLCLIDriver
 See https://github.com/apache/spark/blob/master/sql/hive-thriftserver/src/main/scala/org/apache/spark/sql/hive/thriftserver/SparkSQLCLIDriver.scala
 */
public class StatementSplitter {
    private boolean insideSingleQuote = false;
    private boolean insideDoubleQuote = false;
    private boolean insideSimpleComment = false;
    private int bracketedCommentLevel = 0;
    private boolean escape = false;
    private int beginIndex = 0;
    private boolean leavingBracketedComment = false;
    private boolean isStatement = false;
    private final ArrayList<String> ret = new ArrayList<>();

    private final String line;

    public StatementSplitter(String sql) {
        this.line = sql;
    }

    public List<String> split() {
        for (int index = 0; index < line.length(); index++) {
            char c = line.charAt(index);
            if (leavingBracketedComment) {
                bracketedCommentLevel = -1;
                leavingBracketedComment = false;
            }
            if (c == '\'' && !insideComment()) {
                if (!escape && !insideDoubleQuote) {
                    insideSingleQuote = !insideSingleQuote;
                }
            } else if (c == '\"' && !insideComment()) {
                if (!escape && !insideSingleQuote) {
                    insideDoubleQuote = !insideDoubleQuote;
                }
            } else if (c == '-') {
                boolean hasNext = index + 1 < line.length();
                if (insideDoubleQuote || insideSingleQuote || insideComment()) {
                } else if (hasNext && line.charAt(index + 1) == '-') {
                    insideSimpleComment = true;
                }
            } else if (c == ';') {
                if (insideSingleQuote || insideDoubleQuote || insideComment()) {
                } else {
                    if (isStatement) {
                        ret.add(line.substring(beginIndex, index));
                    }
                    beginIndex = index + 1;
                    isStatement = false;
                }
            } else if (c == '\n') {
                if (!escape) {
                    insideSimpleComment = false;
                }
            } else if (c == '/' && !insideSimpleComment) {
                boolean hasNext = index + 1 < line.length();
                if (insideSingleQuote || insideDoubleQuote) {
                } else if (insideBracketedComment() && line.charAt(index - 1) == '*') {
                    leavingBracketedComment = true;
                } else if (hasNext && !insideBracketedComment() & line.charAt(index + 1) == '*') {
                    bracketedCommentLevel += 1;
                }
            }
            if (escape) {
                escape = false;
            } else if (line.charAt(index) == '\\') {
                escape = true;
            }
            isStatement = statementInProgress(index);
        }
        boolean endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1;
        if (!endOfBracketedComment && (isStatement || insideBracketedComment())) {
            ret.add(line.substring(beginIndex));
        }
        return ret;
    }

    private boolean insideBracketedComment() {
        return bracketedCommentLevel > 0;
    }

    private boolean insideComment() {
        return insideSimpleComment || insideBracketedComment();
    }

    private boolean statementInProgress(int index) {
        return isStatement || (!insideComment() && index > beginIndex && !String.valueOf(line.charAt(index)).trim().isEmpty());
    }
}
