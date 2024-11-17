package com.shsw.pulsar.io.oracle.integration.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data(staticConstructor = "of")
public class ModelTestI {
    private int intField;
    private String textField;

    public ModelTestI() {
    }

    @JsonCreator
    public ModelTestI(@JsonProperty("intField") int intField,
                      @JsonProperty("textField") String textField) {
        this.intField = intField;
        this.textField = textField;
    }
}
