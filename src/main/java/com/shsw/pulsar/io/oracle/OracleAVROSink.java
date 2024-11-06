package com.shsw.pulsar.io.oracle;

import com.google.protobuf.ByteString;
import com.shsw.pulsar.io.oracle.core.OracleAbstractSink;
import com.shsw.pulsar.io.oracle.core.StatementBuilder.ColumnMetaData;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.avro.generic.GenericRecord;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.*;

public class OracleAVROSink extends OracleAbstractSink<GenericObject> {


    @Override
    public void bindValue(PreparedStatement preparedStatement, Record<GenericObject> record) throws Exception {
        // Deserialize record to hashmap field and value.
        Map<String, Object> deserializedAVRO = deserializeRecord(record);

        final List<ColumnMetaData> columns = new ArrayList<>();
        int index = 1;

        switch (oracleConfigs.getInsertMode()) {
            case INSERT -> columns.addAll(tableDefinition.getColumns());
            case UPDATE -> {
                // Add non-key columns first due to WHERE-clause statement
                columns.addAll(tableDefinition.getNonKeyColumns());
                columns.addAll(tableDefinition.getKeyColumns());
            }
        }

        for (ColumnMetaData column : columns) {
            Object value = deserializedAVRO.get(column.getColumnName());
            // Set the statement based on the type of value
            setStatement(preparedStatement, index, value);
            index++;
        }
    }

    /**
     * The function that get the avro-schema from message, grep the field name and value then bind it together.
     * @param record a message from pulsar
     * @return a hash-map of field name and value
     */
    private Map<String, Object> deserializeRecord(Record<GenericObject> record) {
        Map<String, Object> deserializedAVRO = new HashMap<>();

        GenericRecord avroNode = (GenericRecord) record.getValue().getNativeObject();
        for (Schema.Field field : avroNode.getSchema().getFields()) {

            Object value = switch (field.getProp("logicalType")) {
                case "date" -> convertToSQLDate((int) avroNode.get(field.toString()));
                case "decimal" -> convertToBigDecimal((byte[]) avroNode.get(field.toString()), Integer.parseInt(field.getProp("scale")));
                // for non-logical types
                default -> avroNode.get(field.toString());
            };

            deserializedAVRO.put(field.name(), value);
        }
        return deserializedAVRO;
    }

    /**
     * Set the value to given index position on sql statement based on instance of value.
     * @param preparedStatement the given prepared statement
     * @param index the index of binding position
     * @param value the value of given object
     * @throws Exception throws when the object type is not support
     */
    private void setStatement(PreparedStatement preparedStatement, int index, Object value) throws Exception {
        if (value instanceof Integer) {
            preparedStatement.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            preparedStatement.setLong(index, (Long) value);
        } else if (value instanceof Float) {
            preparedStatement.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            preparedStatement.setBoolean(index, (Boolean) value);
        } else if (value instanceof String) {
            preparedStatement.setString(index, value.toString());
        } else if (value instanceof Short) {
            preparedStatement.setShort(index, (Short) value);
        } else if (value instanceof ByteString) {
            preparedStatement.setBytes(index, ((ByteString) value).toByteArray());
        } else if (value instanceof Date) {
            preparedStatement.setDate(index, (Date) value);
        } else if (value instanceof BigDecimal) {
            preparedStatement.setBigDecimal(index, (BigDecimal) value);
        } else {
            throw new Exception("Not support incoming datatype: " + value.getClass());
        }

    }

    /**
     * Helper function for convert int value to date.
     * @param avroDate the integer that contains date.
     * @return Date value
     */
    protected static Date convertToSQLDate(int avroDate) {
        LocalDate localDate = LocalDate.ofEpochDay(avroDate);
        return Date.valueOf(localDate);
    }

    /**
     * Helper function for convert array of bytes and scale into big-decimal type.
     * @param decimalBytes the array of bytes
     * @param scale the scale of decimal position
     * @return BigDecimal value
     */
    protected static BigDecimal convertToBigDecimal(byte[] decimalBytes, int scale) {
        BigInteger unscaledValue = new BigInteger(decimalBytes);
        return new BigDecimal(unscaledValue, scale);
    }
}
