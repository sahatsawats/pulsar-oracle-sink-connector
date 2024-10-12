package com.shsw.pulsar.io.oracle;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;



@Slf4j
public abstract class OracleAbstractSink<K,V> implements Sink<T> {
    @Getter
    private Connection oracleConn;
    private PreparedStatement preparedStatement;

    OracleSinkConfig oracleConfigs;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        oracleConfigs = OracleSinkConfig.load(config);

        // If one of these configuration is null --> Throws exception.
        if (oracleConfigs.getJdbcURL() == null) {
            throw new IllegalArgumentException("Required jdbc not set.");
        }

        createOracleConnection();
        // In transaction mode, autocommit have to be disable.
        oracleConn.setAutoCommit(!oracleConfigs.isUseTransaction());
        log.info("Open jdbc connection to oracle database: {}, autoCommit: {}", oracleConfigs.getJdbcURL(), oracleConn.getAutoCommit());


    }

    // Create database connection.
    private void createOracleConnection() throws SQLException {
        oracleConn = DriverManager.getConnection(oracleConfigs.getJdbcURL(), oracleConfigs.getUser(), oracleConfigs.getPassword());
    }

    // Objective: Initialize statement with given configurations.
    private void initStatement() throws SQLException {
        preparedStatement = oracleConn.prepareStatement(buildInsertStatement());

        if (oracleConfigs.getInsertMode() == OracleSinkConfig.InsertMode.UPSERT) {

        }



    }

    // Objective: Create an abstract insert statement.
    // Example: INSERT INTO tableName(column1,column2, ..., n) VALUES (?,?,...,?)
    private String buildInsertStatement() {
        List<String> listOfKeyColumns = getListFromConfig(oracleConfigs.getPkColumns());
        List<String> listOfNonKeyColumns = getListFromConfig(oracleConfigs.getNonPKColumns());
        StringBuilder builder = new StringBuilder();
        // Build Insert statement
        builder.append("INSERT INTO ");
        builder.append(oracleConfigs.getTable());
        builder.append("(");
        listOfKeyColumns.forEach(key -> builder.append(key).append(","));
        listOfNonKeyColumns.forEach(nonKey -> builder.append(nonKey).append(","));
        builder.deleteCharAt(-1);
        builder.append(") VALUES (");
        // Loop until exceeds the number of sum between PK and nonPK with -1
        for (int i = 0; i < (listOfKeyColumns.size() + listOfNonKeyColumns.size() - 1); i++) {
            builder.append("?,");
        }
        builder.append("?)");

        return builder.toString();
    }

    // Objective: Convert string from configuration file to list of strings with comma-delimiter.
    private List<String> getListFromConfig(String str) {
        return Arrays.stream(str.split(",")).toList();
    }



}
