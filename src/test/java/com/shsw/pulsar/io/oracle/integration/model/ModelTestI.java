package com.shsw.pulsar.io.oracle.integration.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
public class ModelTest {
    private int intField;
    private String textField;
    private BigDecimal bigDecimalField;
    private Date dateField;
}
