package com.shsw.pulsar.io.oracle;

import java.sql.*;

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
    public void write(Record<T> record) {
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

    // Initialize statement with given configurations.
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


    // Convert string from configuration file to list of strings with comma-delimiter.
    private List<String> getListFromConfig(String str) {
        return Arrays.stream(str.split(",")).toList();
    }

    // Use to binding the incoming messages to preparedStatement.
    public abstract void bindValue(PreparedStatement preparedStatement, Record<T> record) throws Exception;


    private void flush() {
        /*
        Logic1: check that the size of incoming messages (Records) more than 0
        Logic2: checking the state of flushing, if current value is "false" and successfully update to true -> return true, either false.
         */
        if (incomingList.size() > 0 && isFlushing.compareAndSet(false, true)) {
            boolean isNeedAnotherRound = false;

            final Deque<Record<T>> bufferList = new LinkedList<>();

            synchronized (incomingList) {
                // actualBatchSize required for loop dequeue record in incomingList.
                 /*
                 This logic required for scheduling the another flush. for following scenario:
                 Normal scenario: (incomingList < batchSize) -> use incomingList size to prevents out of bonds.
                 Exceed batch size scenario: (incomingList > batchSize) -> use batchSize, this will trigger the 2nd flush later.
                 */
                final int actualBatchSize;
                if (batchSize > 0) {
                    actualBatchSize = Math.min(incomingList.size(), batchSize);
                } else {
                    actualBatchSize = incomingList.size();
                }

                // Swap the record to buffer list
                for (int i = 0; i < actualBatchSize; i++) {
                    bufferList.add(incomingList.removeFirst());
                }

                // If batch enabled, incoming list is not empty and meet the batch size (Exceed scenario) -> require 2nd flush.
                if (batchSize > 0 && !incomingList.isEmpty() && incomingList.size() >= batchSize) {
                    isNeedAnotherRound = true;
                }
            }

            int count = 0;
            try {
                // Get the template prepared statement
                PreparedStatement bindStatement = preparedStatement;

                for (Record<T> record : bufferList) {
                    // Called bind method for binding record to statement
                    bindValue(bindStatement, record);
                    // Increment count integer to keep tracking.
                    count += 1;

                    // If batch enable -> add current statement to batch. If not, execute on each statement.
                    if (oracleConfigs.isUseJDBCBatch()) {
                        // Add current statement to queue.
                        bindStatement.addBatch();
                    } else {
                        bindStatement.execute();
                        // If transaction is not enable -> acknowledge each on record.
                        if (!oracleConfigs.isUseTransaction()) {
                            bufferList.removeFirst().ack();
                        }
                    }

                    if (oracleConfigs.isUseJDBCBatch()) {
                        log.debug("Execute batch with total {} statement", count );
                        executeBatch(bufferList, bindStatement);
                    } else if (oracleConfigs.isUseTransaction()) {
                        oracleConn.commit();
                        bufferList.forEach(Record::ack);
                    }
                }
            } catch (Exception e) {
                log.error("Got exception {} - {}", e.getClass().getName(), e.getMessage());
                // Negative acknowledge
                bufferList.forEach(Record::fail);
                try {
                    if (oracleConfigs.isUseTransaction()) {
                        oracleConn.rollback();
                    }
                } catch (Exception ee) {
                    throw new RuntimeException(ee);
                }
            }

            isFlushing.set(false);
            if (isNeedAnotherRound) {
                flush();
            }

        } else {
            log.debug("Already in flush state with queue size: {}", incomingList.size());
        }

    }

    // Execute statement in batch. Ack messages and if transaction is enabled -> commit. If got exception -> nack, rollback, and raise sql exception.
    private void executeBatch(Deque<Record<T>> bufferList, PreparedStatement bindStatement) throws Exception {
        final int[] results = bindStatement.executeBatch();
        int err_counts = 0;

        for (int result : results) {
            if (result == Statement.EXECUTE_FAILED) {
                err_counts += 1;
            }
        }

        if (err_counts > 0) {
            if (oracleConfigs.isUseTransaction()) {
                oracleConn.commit();
            }
            for (int ignored : results) {
                bufferList.removeFirst().ack();
            }
        } else {
            if (oracleConfigs.isUseTransaction()) {
                oracleConn.rollback();
            }
            for (int ignored : results) {
                bufferList.removeFirst().fail();
            }
            String errorMsg = "Batch Execute failed with total error {} times" + err_counts;
            // Throw this exception will be caught in flush() which will nack the messages.
            throw new SQLException(errorMsg);
        }

    }
}
