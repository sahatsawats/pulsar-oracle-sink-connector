package com.shsw.pulsar.io.oracle;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.sql.Date;
import java.time.LocalDate;

public class OracleAVROSinkUtilsTest {

    @Test
    public void convertToSQLDateTest() {
        // Avro logical type, example: 2022-05-05
        int avroDate = 19125;
        Date sqlDate = Date.valueOf(LocalDate.ofEpochDay(avroDate));
        Date receivedFromFunc = OracleAVROSink.convertToSQLDate(avroDate);
        Assert.assertEquals(sqlDate, receivedFromFunc);
    }

    @Test
    public void convertToBigDecimalTest() {
        // Represent the value of 1234567 and scale is equal 2, example: 12345.67
        byte[] decimalBytes = new byte[] { (byte) 0x12, (byte) 0xD6, (byte) 0x87};
        int scale = 2;
        java.math.BigDecimal sqlBigDecimal = new java.math.BigDecimal("12345.67");
        java.math.BigDecimal receivedFromFunc = OracleAVROSink.convertToBigDecimal(decimalBytes, scale);

        Assert.assertEquals(sqlBigDecimal, receivedFromFunc);
    }
}