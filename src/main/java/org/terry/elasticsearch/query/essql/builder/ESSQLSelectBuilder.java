package org.terry.elasticsearch.query.essql.builder;

public interface ESSQLSelectBuilder {
    ESSQLSelectBuilder select(String... fields);

    ESSQLSelectBuilder selectWithAlias(String field, String alias);

    ESSQLSelectBuilder from(String expr);

    ESSQLSelectBuilder orderBy(String... fields);

    ESSQLSelectBuilder groupBy(String expr);

    ESSQLSelectBuilder limit(int rowCount);

    ESSQLSelectBuilder where(String expr);

    ESSQLSelectBuilder whereAnd(String expr);

    ESSQLSelectBuilder whereOr(String expr);

    String toString();
}
