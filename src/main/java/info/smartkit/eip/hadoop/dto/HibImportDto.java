package info.smartkit.eip.hadoop.dto;

import java.io.Serializable;

/**
 * Created by yangboz on 11/4/15.
 */
public class HibImportDto implements Serializable {
    private String input;
    private String output;
    private Boolean overwrite;
    private String format;//hdfs,file

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
