package com.shsw.pulsar.io.oracle;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class StatementBuilder {

    @Data(staticConstructor = "of")
    public static class TableMetaData {
        private final String schemaName;
        private final String tableName;
    }

    @Data(staticConstructor = "of")
    public static class ColumnMetaData {
        private final String columnName;
        // Used for bind value to statement with statement.set
        private final Integer dataType;
        private final String dataTypeName;
        // Used for bind value to the column index
        private final Integer position;
    }


    @Getter
    @Setter
    @ToString
    public static class TableDefinition {
        // Holding the total columns in table
        private final TableMetaData tableMetaData;
        private final List<ColumnMetaData> columns;
        private final List<ColumnMetaData> keyColumns;
        private final List<ColumnMetaData> nonKeyColumns;


        private TableDefinition(TableMetaData tableMetaData,
                                List<ColumnMetaData> columns) {
            this(tableMetaData, columns, null, null);
        }

        private TableDefinition(TableMetaData tableMetaData,
                                List<ColumnMetaData> columns,
                                List<ColumnMetaData> keyColumns,
                                List<ColumnMetaData> nonKeyColumns) {
            this.tableMetaData = tableMetaData;
            this.columns = columns;
            this.keyColumns = keyColumns;
            this.nonKeyColumns = nonKeyColumns;
        }

        // Static factory method: returning an instance of class.
        public static TableDefinition of(TableMetaData tableMetaData, List<ColumnMetaData> columns) {
            return new TableDefinition(tableMetaData, columns);
        }
        public static TableDefinition of(TableMetaData tableMetaData,
                                         List<ColumnMetaData> columns,
                                         List<ColumnMetaData> keyColumns,
                                         List<ColumnMetaData> nonKeyColumns) {
            return new TableDefinition(tableMetaData, columns, keyColumns, nonKeyColumns);
        }

    }


    // Objective: Query metadata and check that is match with provided data.
    public static TableMetaData getTableMetaData(Connection connection, String schema, String table) throws Exception {
        // Query metadata with given schema and table name.
        try(ResultSet resultSet = connection.getMetaData().getTables(null,
                schema,
                table,
                new String[]{"TABLE", "PARTITIONED TABLE"})) {
            if(resultSet.next()) {
                String querySchemaName = resultSet.getString("TABLE_SCHEM");
                String queryTableName = resultSet.getString("TABLE_NAME");

                // If the queried data and given is mismatch. -> raise an error
                if (!Objects.equals(queryTableName, table) || !Objects.equals(querySchemaName, schema)) {
                    throw new IllegalArgumentException("Given schema or table name is mismatch with query results.");
                }

                return TableMetaData.of(querySchemaName, queryTableName);
            } else {
                throw new Exception("Not able to find table: " + table);
            }

        }
    }

    // Objective: Get the table metadata include columns.
    public static TableDefinition getTableDefinition(Connection connection, TableMetaData tableMetaData,
                                                     List<String> keyColumns, List<String> nonKeyColumns) throws Exception {
        // Initialize TableDefinition class
        TableDefinition tableDefinition = TableDefinition.of(tableMetaData, List.of(), List.of(), List.of());

        try(ResultSet resultSet = connection.getMetaData().getColumns(null, tableMetaData.getSchemaName(), tableMetaData.getTableName(), null)) {
            // Loop constructs ColumnMetaData and add to the list -> for construct TableDefinition.
            while (resultSet.next()) {
                // Query required fields for initialize ColumnMetaData.
                String columnName = resultSet.getString("COLUMN_NAME");
                Integer dataType = resultSet.getInt("DATA_TYPE");
                String dataTypeName = resultSet.getString("TYPE_NAME");
                Integer position = resultSet.getInt("ORDINAL_POSITION");

                // Checking queried fields is in list of given columns or not.
                // Adding two objects for further use in binding values to statement.
                if (keyColumns.contains(columnName)) {
                    // Construct ColumnMetaData and add to list.
                    tableDefinition.columns.add(ColumnMetaData.of(columnName, dataType, dataTypeName, position));
                    tableDefinition.keyColumns.add(ColumnMetaData.of(columnName, dataType, dataTypeName, position));
                } else if (nonKeyColumns.contains(columnName)) {
                    tableDefinition.columns.add(ColumnMetaData.of(columnName, dataType, dataTypeName, position));
                    tableDefinition.nonKeyColumns.add(ColumnMetaData.of(columnName, dataType, dataTypeName, position));
                }
            }
        }

        return tableDefinition;
    }

    // TODO: Building Insert methods: upsert, insert, update
    // Building Insert Statement
    public String buildInsertStatement(TableDefinition tableDefinition) {
        // String insert format: INSERT INTO TABLE_NAME(field1,...,fieldN) VALUES(?,...,?)
        StringBuilder builder = new StringBuilder();

        builder.append("INSERT INTO ");
        builder.append(tableDefinition.tableMetaData.tableName);
        builder.append("(");
        tableDefinition.columns.forEach(columnName -> builder.append(columnName).append(","));
        builder.deleteCharAt(-1);
        builder.append(") VALUES (");
        for (int i=0; i<(tableDefinition.columns.size() -1); i++) {
            builder.append(("?, "));
        }
        builder.append("?)");

        return builder.toString();
    }

    // Building Update Statement
    public String buildUpdateStatement(TableDefinition tableDefinition) {
        // String update format: UPDATE table_name SET column1 = ?, column2 = ?,.. columnN = ? WHERE keyColumn = ?
        StringBuilder builder = new StringBuilder();

        builder.append("UPDATE ");
        builder.append(tableDefinition.tableMetaData.tableName);
        builder.append(" SET ");

        tableDefinition.columns.forEach(column -> builder.append(column).append(" = ?,"));
        // Delete excesses comma
        builder.deleteCharAt(-1);
        builder.append("WHERE ");

        if (tableDefinition.keyColumns.size() > 1) {
            tableDefinition.keyColumns.forEach(pkColumn -> builder.append(pkColumn).append(" = ? AND "));
            // Delete excesses AND operation
            builder.delete(-4,-1);
        } else {
            tableDefinition.keyColumns.forEach(pkColumn -> builder.append(pkColumn).append("= ?"));
        }

        return builder.toString();
    }

}
