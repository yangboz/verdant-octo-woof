package info.smartkit.eip.hadoop.dto;

import java.io.Serializable;

/**
 * Created by yangboz on 11/6/15.
 */
public class HibDumpDto implements Serializable {
    private String input;
    private String output;

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
}
