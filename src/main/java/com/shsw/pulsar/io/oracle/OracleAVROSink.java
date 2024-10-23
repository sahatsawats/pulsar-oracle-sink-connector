package com.shsw.pulsar.io.oracle;

import com.shsw.pulsar.io.oracle.core.OracleAbstractSink;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;

import java.sql.PreparedStatement;

public class OracleAVROSink extends OracleAbstractSink<GenericObject> {

    @Override
    public void bindValue(PreparedStatement preparedStatement, Record<GenericObject> record) throws Exception {

    }
}
