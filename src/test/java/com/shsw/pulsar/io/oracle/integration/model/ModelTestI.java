package com.shsw.pulsar.io.oracle.integration.model;

import lombok.Data;


@Data(staticConstructor = "of")
public class ModelTestI {
    private int intField;
    private String textField;
}
