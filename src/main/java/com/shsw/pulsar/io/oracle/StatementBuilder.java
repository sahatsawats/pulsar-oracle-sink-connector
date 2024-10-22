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

    /**
     * Query the metadata of table from given schema and table name.
     * @param connection the connection to database
     * @param schema the schema name that contain the table
     * @param table the table name
     * @return Instance of TableMetaData
     * @throws Exception if unable to find table
     */
    public static TableMetaData getTableMetaData(Connection connection, String schema, String table) throws Exception {
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

    /**
     * Query the metadata of each column from tableMetaData, map queried results compare with given data.
     * @param connection the connection to database
     * @param tableMetaData the instance of TableMetaData
     * @param keyColumns the given key columns name from sink configuration
     * @param nonKeyColumns the given non-key columns name from sink configuration
     * @return Instance of TableDefinition that contain metadata of each given columns
     * @throws Exception if unable to find the columns metadata with given schema and table name
     */
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

    /**
     * Building the insert statement with given TableDefinition.
     * @param tableDefinition the TableDefinition that contains metadata of table and columns
     * @return insert statement
     */
    public static String buildInsertStatement(TableDefinition tableDefinition) {
        // String insert format: INSERT INTO TABLE_NAME(field1,...,fieldN) VALUES(?,...,?)
        StringBuilder builder = new StringBuilder();

        builder.append("INSERT INTO ");
        builder.append(tableDefinition.tableMetaData.tableName);
        builder.append("(");
        tableDefinition.columns.forEach(column -> builder.append(column.columnName).append(","));
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") VALUES (");
        for (int i=0; i<(tableDefinition.columns.size()-1); i++) {
            builder.append(("?, "));
        }
        builder.append("?)");

        return builder.toString();
    }

    /**
     * Building the insert statement with given TableDefinition.
     * @param tableDefinition the TableDefinition that contains metadata of table and columns
     * @return update statement
     */
    public static String buildUpdateStatement(TableDefinition tableDefinition) {
        // String update format: UPDATE table_name SET column1 = ?, column2 = ?,.. columnN = ? WHERE keyColumn = ?
        StringBuilder builder = new StringBuilder();

        builder.append("UPDATE ");
        builder.append(tableDefinition.tableMetaData.tableName);
        builder.append(" SET ");

        tableDefinition.nonKeyColumns.forEach(column -> builder.append(column.columnName).append(" = ?,"));
        // Delete excesses comma
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" WHERE ");

        // Checking composite key
        if (tableDefinition.keyColumns.size() > 1) {
            tableDefinition.keyColumns.forEach(pkColumn -> builder.append(pkColumn.columnName).append(" = ? AND "));
            // Delete excesses AND operation, the last loop going to leave the unnecessary "AND " operation.
            builder.delete(builder.length()-5, builder.length());
        } else {
            tableDefinition.keyColumns.forEach(pkColumn -> builder.append(pkColumn.columnName).append(" = ?"));
        }

        return builder.toString();
    }

}
