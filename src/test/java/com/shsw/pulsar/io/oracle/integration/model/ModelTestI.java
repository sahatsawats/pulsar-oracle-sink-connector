package com.shsw.pulsar.io.oracle.integration.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;


@Data(staticConstructor = "of")
public class ModelTestI {
    private int intField;
    private String textField;
    private BigDecimal bigDecimalField;
    private Date dateField;
}
