package com.shsw.pulsar.io.oracle;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class OracleSinkConfig implements Serializable {
    // Actually, I don't understand why the configuration file have to be serialized.
    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A user of oracle host to connect to"
    )
    private String user;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A password for user"
    )
    private String password;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The JDBC url of the database this connector connects to"
    )
    private String jdbcURL;

    @FieldDoc(
            required = true,
            defaultValue = "username",
            help = "A name of schema that include table"
    )
    private String schema = this.getUser();

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A name of table within scheme that connect to"
    )
    private String table;

    @FieldDoc(
            defaultValue = "",
            help = "A set of PK name which define as list. for example: [PK1,PK2]"
    )
    private String keyColumns;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Set of columns name."
    )
    private String nonKeyColumns;

    @FieldDoc(
            defaultValue = "200",
            help = "Enable batch mode by number of operations. This value is the max number of operations "
                    + "batched in the same transaction/batch."
    )
    private int batchSize = 200;

    @FieldDoc(
            defaultValue = "false",
            help = "Use JDBC batch API, increase write performance."
    )
    private boolean useJDBCBatch = false;

    @FieldDoc(
            defaultValue = "true",
            help = "Enable transaction of database."
    )
    private boolean useTransaction = true;

    @FieldDoc(
            defaultValue = "INSERT",
            help = "The insert mode: INSERT, UPDATE."
    )
    private InsertMode insertMode = InsertMode.INSERT;

    @FieldDoc(
            defaultValue = "500",
            help = "Enable batch mode by time. After timeout, the operations queue will be flushed."
    )
    private Integer timeoutMS = 500;


    public enum InsertMode {
        INSERT,
        UPDATE
    }

    public static OracleSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), OracleSinkConfig.class);
    }

    public static OracleSinkConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(config), OracleSinkConfig.class);
    }

    public void validateBatch() {
        if (timeoutMS <= 0 && batchSize <= 0) {
            throw new IllegalArgumentException("timeoutMs and batchSize must be set to a positive value.");
        }
    }

}
