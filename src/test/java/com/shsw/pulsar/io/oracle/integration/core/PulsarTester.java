package com.shsw.pulsar.io.oracle.integration.core;

import lombok.Getter;
import org.apache.pulsar.client.api.PulsarClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
public abstract class PulsarTester<ServiceContainerT extends GenericContainer<?>> {
    protected final Network sharedNetwork = Network.newNetwork();
    protected final String networkAlias;
    protected final String sinkArchive;
    protected final String sinkClassName;
    protected final Map<String, Object> sinkConfig;
    protected GenericContainer<?> pulsarContainer;
    protected ServiceContainerT serviceContainer;

    public PulsarTester(String networkAlias, String sinkArchive, String sinkClassName) {
        this.networkAlias = networkAlias;
        this.sinkArchive = sinkArchive;
        this.sinkClassName = sinkClassName;
        this.sinkConfig = new HashMap<>();
    }

    /**
     * An abstract method to create a destination container (service), used to be called via startContainerService
     * @return a service container
     */
    protected abstract ServiceContainerT createContainerService();

    /**
     * A method that return the pulsar container
     * @param imageName image name of pulsar
     * @return pulsar container
     */
    protected GenericContainer<?> createPulsarContainerService(String imageName) {
        return new GenericContainer<>(DockerImageName.parse(imageName))
                .withStartupTimeout(Duration.ofMinutes(3))
                .withExposedPorts(6650)
                .withExposedPorts(8080)
                .withNetwork(sharedNetwork)
                .withNetworkAliases("pulsar");
    }

    /**
     * A preparation method for sink container (service), used for create a necessary compartment such as configurations, connection, tables,...
     */
    public abstract void prepareSink() throws Exception;

    /**
     * A method to validate the results
     * @param results is a key-value of produced value
     */
    public abstract void validateSinkResult(Map<String, Object> results);

    /**
     * A method that used to initialize pulsar service and sink service.
     * @param dockerImageName a image name of pulsar service, for different version
     */
    public void startContainerServices(String dockerImageName) {
        this.serviceContainer = createContainerService();
        this.pulsarContainer = createPulsarContainerService(dockerImageName);
    }

    /**
     * A method that return the sink configuration
     * @return a sink configurations
     */
    public Map<String, Object> getSinkConfig() {
        return sinkConfig;
    }

    /**
     * An abstract method for produce messages
     * @param numMessage number of messages that will produce
     * @param inputTopicName a name of topic that produced
     * @param client a pulsar client that connect to container
     * @return hashmap of messages, use for validation
     */
    public abstract LinkedHashMap<String, Object> produceMessages(int numMessage, String inputTopicName, PulsarClient client) throws Exception;

    /**
     * A method that use for close the container services
     */
    public void stopContainerServices() {
        if (serviceContainer != null) {
            serviceContainer.stop();
            pulsarContainer.stop();
        }
        serviceContainer = null;
        pulsarContainer = null;
    }

}
