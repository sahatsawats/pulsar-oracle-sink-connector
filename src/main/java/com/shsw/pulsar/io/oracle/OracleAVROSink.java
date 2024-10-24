package com.shsw.pulsar.io.oracle;

import com.google.protobuf.ByteString;
import com.shsw.pulsar.io.oracle.core.OracleAbstractSink;
import com.shsw.pulsar.io.oracle.core.StatementBuilder.ColumnMetaData;
import oracle.jdbc.proxy.annotation.Pre;
import oracle.sql.DATE;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.avro.generic.GenericRecord;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleAVROSink extends OracleAbstractSink<GenericObject> {

    @Override
    public void bindValue(PreparedStatement preparedStatement, Record<GenericObject> record) throws Exception {

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
            setStatement(preparedStatement, index, value);
            index++;
        }

    }

    private Map<String, Object> deserializeRecord(Record<GenericObject> record) {
        Map<String, Object> deserializedAVRO = new HashMap<>();

        GenericRecord avroNode = (GenericRecord) record.getValue().getNativeObject();
        for (Schema.Field field : avroNode.getSchema().getFields()) {
            deserializedAVRO.put(field.name(), avroNode.get(field.toString()));
        }
        return deserializedAVRO;
    }

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
        } else {
            throw new Exception("Not support incoming datatype: " + value.getClass());
        }
    }
}
