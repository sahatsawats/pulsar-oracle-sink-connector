package com.shsw.pulsar.io.oracle.integration.core;

import org.testcontainers.oracle.OracleContainer;

import java.sql.Connection;
import java.time.Duration;

// TODO: Do prepareSink
public abstract class OracleSinkTester extends PulsarTester<OracleContainer> {
    private Connection connection;
    private final String tableName;

    public OracleSinkTester(String sinkArchive, String className, String tableName) {
        super("oracle", sinkArchive, className);

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
    public void prepareSink() throws Exception {
        String jdbcUrl = serviceContainer.getJdbcUrl();
        sinkConfig.put("jdbcURL", jdbcUrl);
        sinkConfig.put("user", serviceContainer.getUsername());
        sinkConfig.put("password", serviceContainer.getPassword());
        sinkConfig.put("table", tableName);
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param keyColumns is set of PK name which define as: [PK1,PK2,...]
     */
    public void setKeyColumns(String keyColumns) {
        sinkConfig.put("keyColumns", keyColumns);
    }

    /**
     * A method that set the "keyColumns" field in sink configuration
     * @param nonKeyColumns is set of non-pk name which define as: [key1,key2,...]
     */
    public void setNonKeyColumns(String nonKeyColumns) {
        sinkConfig.put("nonKeyColumns", nonKeyColumns);
    }

}
