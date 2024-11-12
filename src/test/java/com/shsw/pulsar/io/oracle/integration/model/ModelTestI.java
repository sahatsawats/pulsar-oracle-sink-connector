package com.shsw.pulsar.io.oracle.integration.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data(staticConstructor = "of")
public class ModelTestI {
    private int intField;
    private String textField;
}
