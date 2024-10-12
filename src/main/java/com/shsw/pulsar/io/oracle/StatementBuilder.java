package com.shsw.pulsar.io.oracle;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class StatementBuilder {

    @Data(staticConstructor = "of")
    public static class TableMetaData {
        private String schemaName;
        private String tableName;
    }

    @Data(staticConstructor = "of")
    public static class ColumnMetaData {
        private String columnName;
        private String dataType;
        private Integer position;
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


}
