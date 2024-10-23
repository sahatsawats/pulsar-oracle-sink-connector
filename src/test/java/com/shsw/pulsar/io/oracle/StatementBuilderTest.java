package com.shsw.pulsar.io.oracle;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.shsw.pulsar.io.oracle.core.StatementBuilder.ColumnMetaData;
import com.shsw.pulsar.io.oracle.core.StatementBuilder.TableMetaData;
import com.shsw.pulsar.io.oracle.core.StatementBuilder.TableDefinition;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

public class StatementBuilderTest {

    /**
     * Tests the functionality of building insert statement.
     * This test verifies that the method correctly return the insert statement based on given metadata.
     */
    @Test
    public void buildInsertStatementTest() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        TableMetaData tableMetaData = TableMetaData.of("example", "test_table");

        columnMetaDataList.add(ColumnMetaData.of("id", 1,  "", 1));
        columnMetaDataList.add(ColumnMetaData.of("firstname", 1,  "", 2));
        columnMetaDataList.add(ColumnMetaData.of("lastname", 1,  "", 3));

        TableDefinition tableDefinition = TableDefinition.of(tableMetaData, columnMetaDataList);

        String insertStatement = StatementBuilder.buildInsertStatement(tableDefinition);
        String targetInsertStatement = "INSERT INTO test_table(id,firstname,lastname) VALUES (?, ?, ?)";
        Assert.assertEquals(insertStatement, targetInsertStatement);
    }

    /**
     * Tests the functionality of building update statement with one key column.
     * This test verifies that the method correctly return the update statement based on given metadata.
     */
    @Test
    public void buildUpdateStatementOneKeyTest() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        List<ColumnMetaData> keyColumnMetaDataList = new ArrayList<>();
        List<ColumnMetaData> nonKeyColumnMetaDataList = new ArrayList<>();
        // TableMetaData
        TableMetaData tableMetaData = TableMetaData.of("example", "test_table");
        keyColumnMetaDataList.add(ColumnMetaData.of("id", 1,  "", 1));
        nonKeyColumnMetaDataList.add(ColumnMetaData.of("firstname", 1,  "", 2));
        nonKeyColumnMetaDataList.add(ColumnMetaData.of("lastname", 1,  "", 3));
        TableDefinition tableDefinition = TableDefinition.of(tableMetaData, columnMetaDataList, keyColumnMetaDataList, nonKeyColumnMetaDataList);
        String updateStatement = StatementBuilder.buildUpdateStatement(tableDefinition);
        String targetUpdateStatement = "UPDATE test_table SET firstname = ?,lastname = ? WHERE id = ?";
        Assert.assertEquals(updateStatement, targetUpdateStatement);
    }

    /**
     * Tests the functionality of building update statement with multiple key columns.
     * This test verifies that the method correctly return the update statement based on given metadata.
     */
    @Test
    public void buildUpdateStatementMultipleKeyTest() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        List<ColumnMetaData> keyColumnMetaDataList = new ArrayList<>();
        List<ColumnMetaData> nonKeyColumnMetaDataList = new ArrayList<>();
        // TableMetaData
        TableMetaData tableMetaData = TableMetaData.of("example", "test_table");
        keyColumnMetaDataList.add(ColumnMetaData.of("id", 1,  "", 1));
        keyColumnMetaDataList.add(ColumnMetaData.of("branch", 1, "", 2));
        nonKeyColumnMetaDataList.add(ColumnMetaData.of("firstname", 1,  "", 3));
        nonKeyColumnMetaDataList.add(ColumnMetaData.of("lastname", 1,  "", 4));
        TableDefinition tableDefinition = TableDefinition.of(tableMetaData, columnMetaDataList, keyColumnMetaDataList, nonKeyColumnMetaDataList);
        String updateStatement = StatementBuilder.buildUpdateStatement(tableDefinition);
        String targetUpdateStatement = "UPDATE test_table SET firstname = ?,lastname = ? WHERE id = ? AND branch = ?";
        Assert.assertEquals(updateStatement, targetUpdateStatement);
    }
}