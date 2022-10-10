package io.github.reata.sqllineage4j.common.utils;

public final class Helper {
    public static String escapeIdentifierName(String name) {
        return name.replaceAll("`", "").replaceAll("'", "").replaceAll("\"", "");
    }
}
