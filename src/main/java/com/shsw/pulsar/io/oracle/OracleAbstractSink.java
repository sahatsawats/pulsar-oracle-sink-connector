package com.shsw.pulsar.io.oracle;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import com.shsw.pulsar.io.oracle.StatementBuilder.*;


@Slf4j
public abstract class OracleAbstractSink<T> implements Sink<T> {
    @Getter
    private Connection oracleConn;
    private PreparedStatement preparedStatement;
    OracleSinkConfig oracleConfigs;
    TableDefinition tableDefinition;

    // Flush parameters
    // Deque is a queue that can be dequeue an item from both sides
    private Deque<Record<T>> incomingList;
    // Parameter that controlling the flush systems.
    private AtomicBoolean isFlushing;
    private Integer batchSize;
    // Used for interval execute flush process.
    private ScheduledExecutorService flushScheduleExecutor;


    /*
     This method called when the connector is initialized.
     This method should be used to gather the configuration/runtime-resources, establish connection and so on.
     */
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

        initStatement();

        // Getting the configurations for flush service
        int timeoutMs = oracleConfigs.getTimeoutMS();
        batchSize = oracleConfigs.getBatchSize();
        incomingList = new LinkedList<>();
        isFlushing = new AtomicBoolean(false);

        // Create a concurrent thread that running the flush with scheduled.
        flushScheduleExecutor = Executors.newScheduledThreadPool(1);
        if (timeoutMs > 0) {
            // scheduleAtFixedRate: (runnable, initialDelay (Time to wait before first execution), period (Repeat execution), Time unit)
            flushScheduleExecutor.scheduleAtFixedRate(this::flush, timeoutMs, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /*
    This method called when messages arrives.
    You can decide how to write the data to destination service such as create a buffers, or real-time processing.
     */
    @Override
    public void write(Record<T> record) throws Exception {
        // Declare variable to holding the incoming size.
        int preBufferSize;
        synchronized (incomingList) {
            // Adding the new record to incomingList (Pre-buffer)
            incomingList.add(record);
            preBufferSize = incomingList.size();
        }
        // Logic: If the pre-buffer-size is more-than/equal to the bash size -> execute flush.
        // In term that the batch size is 0, it has to execute via scheduler.
        if (batchSize > 0 && preBufferSize >= batchSize) {
            flushScheduleExecutor.schedule(this::flush, 0, TimeUnit.MILLISECONDS);
        }
    }

    /*
    Helper methods
     */
    // Create database connection.
    private void createOracleConnection() throws SQLException {
        oracleConn = DriverManager.getConnection(oracleConfigs.getJdbcURL(), oracleConfigs.getUser(), oracleConfigs.getPassword());
    }

    // Objective: Initialize statement with given configurations.
    private void initStatement() throws Exception {
        TableMetaData tableMetaData = StatementBuilder.getTableMetaData(oracleConn, oracleConfigs.getSchema(), oracleConfigs.getTable());
        tableDefinition = StatementBuilder.getTableDefinition(oracleConn, tableMetaData,
                getListFromConfig(oracleConfigs.getKeyColumns()), getListFromConfig(oracleConfigs.getNonKeyColumns()));


        if (oracleConfigs.getInsertMode() == OracleSinkConfig.InsertMode.INSERT) {
            preparedStatement = oracleConn.prepareStatement(StatementBuilder.buildInsertStatement(tableDefinition));
        } else if (oracleConfigs.getInsertMode() == OracleSinkConfig.InsertMode.UPDATE) {
            preparedStatement = oracleConn.prepareStatement(StatementBuilder.buildUpdateStatement(tableDefinition));
        } else {
            throw new IllegalArgumentException("Cannot match Insert mode, received: " + oracleConfigs.getInsertMode());
        }


    }


    // Objective: Convert string from configuration file to list of strings with comma-delimiter.
    private List<String> getListFromConfig(String str) {
        return Arrays.stream(str.split(",")).toList();
    }

    // Objective: Use to binding the incoming messages to preparedStatement.
    public abstract void bindValue(PreparedStatement preparedStatement) throws Exception;


    /*
    Flush service
     */
    private void flush() {
        // Logic1: Check that the size of incoming messages (Records) more than 0
        // Logic2: Checking the state of flushing, if current value is "false" and successfully update to true -> return true, either false.
        if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
            boolean isNeedAnotherRound;

            // Idea: Unloading the incoming messages to bufferList
            final Deque<Record<T>> bufferList = new LinkedList<>();

            synchronized (incomingList) {
                /*
                If enable batch (batch size > 0) -> find the smaller one between incomingList and batchSize (not overdo)
                If batch size = 0 -> process all the record in incomingList
                 */
                final int actualBatchSize;
                if (batchSize > 0) {
                    actualBatchSize = Math.min(incomingList.size(), batchSize);
                } else {
                    actualBatchSize = incomingList.size();
                }

                // Swap the record to buffer list
                for (int i=0; i < actualBatchSize; i++) {
                    bufferList.add(incomingList.removeFirst());
                }

                // Require to flush another round?, If incoming list is not empty and meet the batch size:
                if (batchSize > 0 && !incomingList.isEmpty() && incomingList.size() >= batchSize) {
                    isNeedAnotherRound = true;
                }
            }
        }
    }
}
