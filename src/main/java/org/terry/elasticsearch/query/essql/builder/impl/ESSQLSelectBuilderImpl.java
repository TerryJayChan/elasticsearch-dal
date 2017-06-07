package org.terry.elasticsearch.query.essql.builder.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectGroupBy;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.terry.elasticsearch.query.essql.builder.ESSQLSelectBuilder;

public class ESSQLSelectBuilderImpl implements ESSQLSelectBuilder {

    private SQLSelectStatement stmt;
    private String dbType;

    public ESSQLSelectBuilderImpl() {
        stmt = new SQLSelectStatement();
        dbType = JdbcConstants.MYSQL;
    }

    @Override
    public ESSQLSelectBuilder select(String... fields) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        for (String column : fields) {
            SQLSelectItem selectItem = SQLUtils.toSelectItem(column, dbType);
            queryBlock.addSelectItem(selectItem);
        }

        return this;
    }

    @Override
    public ESSQLSelectBuilder selectWithAlias(String field, String alias) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr columnExpr = SQLUtils.toSQLExpr(field, dbType);
        SQLSelectItem selectItem = new SQLSelectItem(columnExpr, alias);
        queryBlock.addSelectItem(selectItem);

        return this;
    }

    @Override
    public ESSQLSelectBuilder from(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();
        SQLExprTableSource from = new SQLExprTableSource(new SQLIdentifierExpr(expr), null);
        queryBlock.setFrom(from);

        return this;
    }

    @Override
    public ESSQLSelectBuilder orderBy(String... fields) {
        SQLSelect select = this.getSQLSelect();

        SQLOrderBy orderBy = select.getOrderBy();
        if (orderBy == null) {
            orderBy = createOrderBy();
            select.setOrderBy(orderBy);
        }

        for (String column : fields) {
            SQLSelectOrderByItem orderByItem = SQLUtils.toOrderByItem(column, dbType);
            orderBy.addItem(orderByItem);
        }

        return this;
    }

    @Override
    public ESSQLSelectBuilder groupBy(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
        if (groupBy == null) {
            groupBy = createGroupBy();
            queryBlock.setGroupBy(groupBy);
        }

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        groupBy.addItem(exprObj);

        return this;
    }

    @Override
    public ESSQLSelectBuilder limit(int rowCount) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        MySqlSelectQueryBlock mySqlQueryBlock = (MySqlSelectQueryBlock) queryBlock;
        MySqlSelectQueryBlock.Limit limit = new MySqlSelectQueryBlock.Limit();
        limit.setRowCount(new SQLIntegerExpr(rowCount));
        mySqlQueryBlock.setLimit(limit);

        return this;
    }

    @Override
    public ESSQLSelectBuilder where(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        queryBlock.setWhere(exprObj);

        return this;
    }

    @Override
    public ESSQLSelectBuilder whereAnd(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        SQLExpr newCondition = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, exprObj, false,
                queryBlock.getWhere());
        queryBlock.setWhere(newCondition);

        return this;
    }

    @Override
    public ESSQLSelectBuilder whereOr(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        SQLExpr newCondition = SQLUtils.buildCondition(SQLBinaryOperator.BooleanOr, exprObj, false,
                queryBlock.getWhere());
        queryBlock.setWhere(newCondition);

        return this;
    }

    public SQLSelect getSQLSelect() {
        if (stmt.getSelect() == null) {
            stmt.setSelect(createSelect());
        }
        return stmt.getSelect();
    }

    public String toString() {
        return SQLUtils.toSQLString(stmt, dbType);
    }

    protected SQLSelectQueryBlock getQueryBlock() {
        SQLSelect select = getSQLSelect();
        SQLSelectQuery query = select.getQuery();
        if (query == null) {
            query = createSelectQueryBlock();
            select.setQuery(query);
        }

        if (!(query instanceof SQLSelectQueryBlock)) {
            throw new IllegalStateException("not support from, class : " + query.getClass().getName());
        }

        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
        return queryBlock;
    }

    protected SQLSelect createSelect() {
        return new SQLSelect();
    }

    protected SQLOrderBy createOrderBy() {
        return new SQLOrderBy();
    }

    protected SQLSelectQuery createSelectQueryBlock() {
        return new MySqlSelectQueryBlock();
    }

    protected SQLSelectGroupByClause createGroupBy() {
        return new MySqlSelectGroupBy();
    }

}
