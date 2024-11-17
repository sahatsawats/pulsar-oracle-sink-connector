package com.shsw.pulsar.io.oracle.integration.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shsw.pulsar.io.oracle.integration.model.ModelTestI;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.testcontainers.containers.Container;
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
    protected final String databaseName = "ORCL";
    protected final String tableName;
    protected String keyColumns;
    protected String nonKeyColumns;
    protected String orderColumn;
    protected String createStatement;

    public OracleSinkTester(String sinkArchive, String className, String tableName, Class<T> tClass) {
        super("oracle", sinkArchive, className);
        this.tableName = tableName;
        this.tClass = tClass;
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
        return (OracleContainer) new OracleContainer("gvenzl/oracle-free:slim")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withDatabaseName(databaseName)
                .withUsername("tester")
                .withPassword("test-pwd")
                .withExposedPorts(1521)
                .withNetwork(sharedNetwork)
                .withNetworkAliases(networkAlias);
    }

    @Override
    public void prepareSink() throws Exception {
        // for internal communication in shared network
        String jdbcUrlInternal = "jdbc:oracle:thin:@oracle:1521/ORCL";
        // for external connection (from local to container)
        String jdbcUrl = serviceContainer.getJdbcUrl();
        String username = serviceContainer.getUsername();
        String password = serviceContainer.getPassword();
        String table = tableName.toUpperCase();
        String schema = username.toUpperCase();

        log.info("Got credentials for integration test: \n " +
                "internal_jdbc = {} \n " +
                "external_jdbc = {} \n " +
                "user = {} \n " +
                "schema = {} \n " +
                "table = {} \n ", jdbcUrlInternal, jdbcUrl, username, schema, table);

        // Define sink configurations based on OracleSinkConfig class.
        sinkConfig.put("jdbcURL", jdbcUrlInternal);
        sinkConfig.put("user", username);
        sinkConfig.put("password", password);
        sinkConfig.put("table", table);
        sinkConfig.put("schema", schema);
        setKeyColumnsConfig(keyColumns);
        setNonKeyColumnsConfig(nonKeyColumns);

        // Create jdbc connection
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        log.info("Get connection: {}, jdbcUrl: {}", connection, jdbcUrl);

        // Get the statement method based on table structure.
        int ret = connection.createStatement().executeUpdate(this.createStatement);
        log.info("Created table in jdbc: {}, return value: {}", this.createStatement, ret);

        ResultSet resultSet = connection.getMetaData().getTables(
                null,
                username.toUpperCase(),
                tableName.toUpperCase(),
                new String[]{"TABLE"});

        if(resultSet.next()) {
            log.info("Create table {} within schema: {}", resultSet.getString("TABLE_NAME"), resultSet.getString("TABLE_SCHEM"));
        } else {
            log.error(serviceContainer.getLogs());
            throw(new Exception("Cannot get metadata information"));
        }
    }

    /**
     * A method to validate the results
     * @param results is a key-value of produced value
     */
    @Override
    public void validateSinkResult(List<Map<String, Object>> results) {
        log.info("Checking rows count...");
        String checkRowStatement = "SELECT COUNT(*) AS rowCount FROM " + tableName.toUpperCase();

        try (PreparedStatement statement = connection.prepareStatement(checkRowStatement)) {
            ResultSet queryResult = statement.executeQuery();

            if (queryResult.next()) {
                int rowCount = queryResult.getInt("rowCount");
                if (rowCount != results.size()) {
                    log.error(getContainerLog());
                    fail("Row count is not equal to produced messages: \n " + "num_produce: " + results.size() + " \n num_queried: " + rowCount);
                }
            } else {
                fail("No rows found from count statement");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.info("Query table content from oracle server: {}", tableName);
        String queryStatement = "SELECT * FROM " + tableName.toUpperCase() + " ORDER BY " + orderColumn;

        // Sorting the list based on given order column
        results.sort((map1, map2) -> {
            Integer value1 = (Integer) map1.get(orderColumn);
            Integer value2 = (Integer) map2.get(orderColumn);
            return value1.compareTo(value2);
        });

        ResultSet rs;
        try {
            PreparedStatement statement = connection.prepareStatement(queryStatement);
            rs = statement.executeQuery();

            int index = 0;
            while (rs.next()) {
                // Get the Map object from list with index
                Map<String, Object> result = results.get(index);
                // Loop through all key, assert equal the query object with given key name.
                for (String key : result.keySet()) {
                    if (key == null) {
                        fail("Got key result null.");
                    }
                    log.info("Given: {}, Received: {}", result.get(key), rs.getObject(key));
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
    public List<Map<String, Object>> produceMessages(String directoryPath, String inputTopicName, PulsarClient client) throws Exception {
        List<Map<String, Object>> messageLogs = new ArrayList<>();

        @Cleanup
        Producer<T> producer = client.newProducer(Schema.AVRO(tClass)).topic(inputTopicName).create();

        List<String> filePaths = listTestFile(directoryPath);
        for (String path : filePaths) {
            // load .json file and map to POJO class
            T payload = loadTestData(path);
            MessageId messageId = producer.newMessage().value(payload).send();
            log.info("Send message with messageId: {}", messageId);

            Map<String, Object> messageLog = new HashMap<>();

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
     * @param createStatement is given statement provided by user
     */
    public void setCreateTableStatement(String createStatement) {
        this.createStatement = createStatement;
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

    public String getContainerLog() throws Exception {
        StringBuilder containerLog = new StringBuilder();
        containerLog.append("Oracle Container Log: \n\n");
        containerLog.append(serviceContainer.getLogs());
        containerLog.append("\n\n Pulsar Container Log: \n\n");
        containerLog.append(pulsarContainer.getLogs());
        containerLog.append("\n\n Pulsar Function Log: \n\n");
        Container.ExecResult functionLog = pulsarContainer.execInContainer("sh", "-c", "cat logs//functions/public/default/oracle-sink-tester/oracle-sink-tester-0.log");
        containerLog.append(functionLog);

        return containerLog.toString();
    }

}
