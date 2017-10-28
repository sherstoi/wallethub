package com.ef.utils;

import com.mysema.query.sql.*;
import com.mysema.query.sql.dml.SQLInsertClause;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Path;
import com.mysema.query.types.expr.Wildcard;
import com.mysema.query.types.path.PathBuilder;
import com.mysema.query.types.path.StringPath;

import java.util.List;

public class SQLBuilder {

    private static final String ALIAS = "alias";

    /**
     * Build insert query for prepared statement object (with ? in values)
     * for example: insert into schemaName.tableName (col1, col2) values (?, ?)
     * @param schemaName schema name
     * @param tableName table name
     * @param columnNames column name
     * @return generated insert query
     */
    public static String buildInsertQuery(String schemaName, String tableName, List<String> columnNames) {
        SQLTemplates sqlTemplates = initializeSQLBuilder();
        RelationalPath<Object> table = new RelationalPathBase<>(Object.class, ALIAS, schemaName, tableName);
        PathBuilder<Object> pathBuilder = new PathBuilder<>(Object.class, table.getMetadata());
        SQLInsertClause insert = new SQLInsertClause(null, sqlTemplates, table);
        columnNames.forEach( (columnName) -> insert.set(pathBuilder.get(columnName), "?"));

        return insert.toString();
    }

    /**
     * Build select count(*) or just select query for prepared statement object
     * @param schemaName schema name
     * @param tableName table name
     * @param columnNames columns name
     * @return generated select query
     */
    public static String buildSelectQuery(boolean isSelectCount, String schemaName, String tableName, List<String> columnNames) {
        SQLTemplates sqlTemplates = initializeSQLBuilder();
        Path table = new RelationalPathBase<>(Object.class, ALIAS, schemaName, tableName);
        StringPath[] columnsPath = new StringPath[columnNames.size()];

        for (int j = 0; j < columnNames.size(); j++) {
            columnsPath[j] = Expressions.stringPath(table, columnNames.get(j));
        }
        SQLQuery sqlQuery = new SQLQuery(sqlTemplates).from(table);
        sqlQuery.setUseLiterals(true);

        return (isSelectCount) ? sqlQuery.getSQL(Wildcard.count).getSQL() :
                                 sqlQuery.getSQL(columnsPath).getSQL();
    }

    private static SQLTemplates initializeSQLBuilder() {
        SQLTemplates.Builder builder = MySQLTemplates.builder();
        return builder.printSchema().build();
    }
}
