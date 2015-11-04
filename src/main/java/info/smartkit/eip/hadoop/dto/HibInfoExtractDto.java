package info.smartkit.eip.hadoop.dto;

import java.io.Serializable;

/**
 * Created by yangboz on 11/4/15.
 */
public class HibInfoExtractDto implements Serializable {
    private int index;
    private String fileName;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
