package com.shsw.pulsar.io.oracle;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;


public class OracleAVROSinkIT {
    private final Network network = Network.newNetwork();
    private final OracleContainer oracleContainer = new OracleContainer("gvenzl/oracle-free:23.4-slim-faststart")
            .withStartupTimeout(Duration.ofMinutes(3))
            .withUsername("tester")
            .withPassword("testpwd")
            .withExposedPorts(1521)
            .withNetwork(network);
    private final GenericContainer<?> pulsarContainer = new GenericContainer<>(DockerImageName.parse("apachepulsar/pulsar:3.3.2"))
            .withStartupTimeout(Duration.ofMinutes(3))
            .withExposedPorts(6650).withExposedPorts(8080).withNetwork(network);

    protected PulsarClient pulsarClient;
    protected PulsarAdmin pulsarAdmin;
    protected Connection oracleConn;

    @BeforeClass
    public void beforeAll() throws Exception {
        oracleContainer.start();
        pulsarContainer.start();

        setPulsarConnection();
        setOracleConnection();
    }



    @AfterClass
    public void afterAll() throws Exception {
        if (!pulsarClient.isClosed()) {
            pulsarClient.close();
        }
        if (!oracleConn.isClosed()) {
            oracleConn.close();
        }

        oracleContainer.stop();
        pulsarContainer.stop();
    }


    /*
    HELPER FUNCTION
     */
    private void setPulsarConnection() throws PulsarClientException {
        String pulsarContainerHost = pulsarContainer.getHost();
        // For example: pulsar://localhost:6650
        String serviceUrl = "pulsar://" + pulsarContainerHost + ":" + pulsarContainer.getMappedPort(6650);
        // For example: http://localhost:8080
        String httpUrl = "http://" + pulsarContainerHost + ":" + pulsarContainer.getMappedPort(8080);

        // Building the connection
        pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build();
    }

    private void setOracleConnection() throws SQLException {
        String jdbcUrl = oracleContainer.getJdbcUrl();
        String username = oracleContainer.getUsername();
        String password = oracleContainer.getPassword();

        oracleConn = DriverManager.getConnection(jdbcUrl, username, password);
    }

}
