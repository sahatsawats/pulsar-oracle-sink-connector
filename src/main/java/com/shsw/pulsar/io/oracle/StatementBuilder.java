package com.shsw.pulsar.io.oracle;

import lombok.*;
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
        private final String dataType;
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

}
