package com.shsw.pulsar.io.oracle.integration.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shsw.pulsar.io.oracle.integration.model.ModelTestI;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Slf4j
public class OracleSinkTester<T> extends PulsarTester<OracleContainer> {
    protected Class<T> tClass;
    protected Connection connection;
    protected final String tableName;
    protected String keyColumns;
    protected String nonKeyColumns;
    protected String orderColumn;

    public OracleSinkTester(String sinkArchive, String className, String tableName) {
        super("oracle", sinkArchive, className);
        this.tableName = tableName;
    }

    /**
     * A method that set the key column, use for creating create-statement
     * @param keyColumns is set of columns name, define as "kColumn1,kColumn2,..."
     */
    public void setKeyColumns(String keyColumns) {
        this.keyColumns = keyColumns;
    }

    /**
     * A method that set the normal column (non-key), use for creating create-statement
     * @param nonKeyColumns is set of columns name, define as "column1,column2,..."
     */
    public void setNonKeyColumns(String nonKeyColumns) {
        this.nonKeyColumns = nonKeyColumns;
    }

    /**
     * A method that set the order-column for query the test results. The type of this column have to be integer.
     * @param orderColumn is column name of integer column with uniqueness.
     */
    public void setOrderColumn(String orderColumn) {
        this.orderColumn = orderColumn;
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
    public void prepareSink() throws Exception {
        String jdbcUrl = serviceContainer.getJdbcUrl();
        String username = serviceContainer.getUsername();
        String password = serviceContainer.getPassword();
        // Define sink configurations based on OracleSinkConfig class.
        sinkConfig.put("jdbcURL", jdbcUrl);
        sinkConfig.put("user", username);
        sinkConfig.put("password", password);
        sinkConfig.put("table", tableName);
        setKeyColumnsConfig(keyColumns);
        setNonKeyColumnsConfig(nonKeyColumns);

        // Create jdbc connection
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        log.info("get connection: {}, jdbcUrl: {}", connection, jdbcUrl);
        // Get the statement method based on table structure.
        String createTableStatement = setCreateTableStatement(tableName, keyColumns);
        int ret = connection.createStatement().executeUpdate(createTableStatement);
        log.info("created table in jdbc: {}, return value: {}", createTableStatement, ret);
    }

    /**
     * A method to validate the results
     * @param results is a key-value of produced value
     */
    @Override
    public void validateSinkResult(List<Map<String, Object>> results) {
        log.info("Query table content from oracle server: {}", tableName);
        String queryStatement = "SELECT * FROM " + tableName + " ORDER BY " + orderColumn;

        // Sorting the list based on given order column
        results.sort((map1, map2) -> {
            Integer value1 = (Integer) map1.get(orderColumn);
            Integer value2 = (Integer) map2.get(orderColumn);
            return value1.compareTo(value2);
        });

        ResultSet rs;
        try {
            Thread.sleep(1000);
            PreparedStatement statement = connection.prepareStatement(queryStatement);
            rs = statement.executeQuery();

            int index = 0;
            while (rs.next()) {
                // Get the Map object from list with index
                Map<String, Object> result = results.get(index);
                // Loop through all key, assert equal the query object with given key name.
                for (String key : result.keySet()) {
                    assertEquals(result.get(key), rs.getObject(key));
                }
                index++;
            }
        } catch (Exception e) {
            log.error("Got exception: ", e);
            fail("Got exception when execute sql statement: " + e);
        }
    }

    @Override
    public List<HashMap<String, Object>> produceMessages(String directoryPath, String inputTopicName, PulsarClient client) throws Exception {
        List<HashMap<String, Object>> messageLogs = new ArrayList<>();

        @Cleanup
        Producer<T> producer = client.newProducer(Schema.AVRO(tClass)).topic(inputTopicName).create();

        List<String> filePaths = listTestFile(directoryPath);
        for (String path : filePaths) {
            // load .json file and map to POJO class
            T payload = loadTestData(path);
            producer.newMessage().value(payload).send();

            HashMap<String, Object> messageLog = new HashMap<>();

            for (Field field : payload.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                messageLog.put(field.getName(), field.get(payload));
            }
            messageLogs.add(messageLog);
        }
        return messageLogs;
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param keyColumns is set of PK name which define as: "PK1,PK2,..."
     */
    protected void setKeyColumnsConfig(String keyColumns) {
        sinkConfig.put("keyColumns", keyColumns);
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param nonKeyColumns is set of non-pk name which define as: key1,key2,...
     */
    protected void setNonKeyColumnsConfig(String nonKeyColumns) {
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
