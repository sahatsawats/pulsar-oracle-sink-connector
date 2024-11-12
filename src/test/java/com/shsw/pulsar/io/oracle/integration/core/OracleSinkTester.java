package com.shsw.pulsar.io.oracle.integration.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shsw.pulsar.io.oracle.integration.model.ModelTestI;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.oracle.OracleContainer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public abstract class OracleSinkTester<T> extends PulsarTester<OracleContainer> {
    protected Class<T> tClass;
    private Connection connection;
    protected final String tableName;

    public OracleSinkTester(String sinkArchive, String className, String tableName, Class<T> tClass) {
        super("oracle", sinkArchive, className);
        this.tClass = tClass;
        this.tableName = tableName;
    }

    @Override
    protected OracleContainer createContainerService() {
        return (OracleContainer) new OracleContainer("gvenzl/oracle-free:23.4-slim-faststart")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withUsername("tester")
                .withPassword("test-pwd")
                .withExposedPorts(1521)
                .withNetwork(sharedNetwork)
                .withNetworkAliases(networkAlias);
    }

    @Override
    public void prepareSink(String keyColumns, String nonKeyColumns) throws Exception {
        String jdbcUrl = serviceContainer.getJdbcUrl();
        String username = serviceContainer.getUsername();
        String password = serviceContainer.getPassword();
        // Define sink configurations based on OracleSinkConfig class.
        sinkConfig.put("jdbcURL", jdbcUrl);
        sinkConfig.put("user", username);
        sinkConfig.put("password", password);
        sinkConfig.put("table", tableName);
        setKeyColumns(keyColumns);
        setNonKeyColumns(nonKeyColumns);

        // Create jdbc connection
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        log.info("get connection: {}, jdbcUrl: {}", connection, jdbcUrl);
        // Get the statement method based on table structure.
        String createTableStatement = setCreateTableStatement(tableName, keyColumns);
        int ret = connection.createStatement().executeUpdate(createTableStatement);
        log.info("created table in jdbc: {}, return value: {}", createTableStatement, ret);
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param keyColumns is set of PK name which define as: "PK1,PK2,..."
     */
    protected void setKeyColumns(String keyColumns) {
        sinkConfig.put("keyColumns", keyColumns);
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param nonKeyColumns is set of non-pk name which define as: key1,key2,...
     */
    protected void setNonKeyColumns(String nonKeyColumns) {
        sinkConfig.put("nonKeyColumns", nonKeyColumns);
    }

    /**
     * A method that used to define the structure of test table based on given class
     * @param tableName is name of test table
     * @param pkColumns is string of key columns
     * @return a string of create table statement
     */
    protected String setCreateTableStatement(String tableName, String pkColumns) {
        List<String> pkList = getListFromConfig(pkColumns);
        StringBuilder statement = new StringBuilder();
        // CREATE TABLE table_name(
        statement.append("CREATE TABLE ").append(tableName).append("(");
        // CREATE TABLE table_name(field1,field2,field3,
        for (Field field : tClass.getDeclaredFields()) {
            statement.append(field.getName());
            statement.append(",");
        }
        // CREATE TABLE table_name(field1,field2,field3
        statement.deleteCharAt(statement.length() - 1);
        // CREATE TABLE table_name(field1,field2,field3) PRIMARY KEY (
        statement.append(") PRIMARY KEY (");
        // If primary key more than 1 (composite key)
        if (pkList.size() > 1) {
            for (String key : pkList) {
                statement.append(key);
                statement.append(",");
            }
            statement.deleteCharAt(statement.length() - 1);
        } else {
            statement.append(pkList.get(0));
        }
        statement.append("))");

        return statement.toString();
    }

    private List<String> getListFromConfig(String str) {
        return Arrays.stream(str.split(",")).toList();
    }

    public List<String> listTestFile(String dirPath) throws IOException {
        try (Stream<Path> paths = Files.list(Paths.get(dirPath))) {
            return paths.filter(Files::isRegularFile)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
    }

    public T loadTestData(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(filePath), tClass);
    }

}
