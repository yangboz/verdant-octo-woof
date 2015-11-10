package info.smartkit.eip.hadoop.dto;

import java.io.Serializable;

/**
 * Created by yangboz on 11/7/15.
 */
public class HibDownloadDto implements Serializable {
    private Boolean force;//force overwrite if output HIB already exists
    private int numOfNodes;//number of download nodes (default=1) (ignored if --yfcc100m is specified)
    private String format = "yfcc100m";//assume input files are in Yahoo/Flickr CC 100M format

    private String input;
    private String output;

    public Boolean getForce() {
        return force;
    }

    public void setForce(Boolean force) {
        this.force = force;
    }

    public int getNumOfNodes() {
        return numOfNodes;
    }

    public void setNumOfNodes(int numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

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
