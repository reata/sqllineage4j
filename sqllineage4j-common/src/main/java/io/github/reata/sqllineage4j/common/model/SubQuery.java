package io.github.reata.sqllineage4j.common.model;

public final class SubQuery implements DataSet {
    private final String query;
    private final String alias;

    public SubQuery(String query, String alias) {
        this.query = query;
        if (alias == null) {
            alias = "subquery_" + query.hashCode();
        }
        this.alias = alias;
    }

    @Override
    public String toString() {
        return alias;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SubQuery && this.query.equals(((SubQuery) obj).getQuery());
    }

    @Override
    public int hashCode() {
        return query.hashCode();
    }

    public String getQuery() {
        return query;
    }

    public String getAlias() {
        return alias;
    }
}
