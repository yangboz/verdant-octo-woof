package info.smartkit.eip.hadoop.dto;

import java.io.Serializable;

/**
 * Created by yangboz on 11/4/15.
 */
public class HibInfoDto implements Serializable {

    private String input;
    private int imageIndex;
    private HibInfoExtractDto extract;
    private Boolean showMeta;
    private String metaKey;
    private Boolean showExif;

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public Boolean getShowMeta() {
        return showMeta;
    }

    public void setShowMeta(Boolean showMeta) {
        this.showMeta = showMeta;
    }

    public Boolean getShowExif() {
        return showExif;
    }

    public void setShowExif(Boolean showExif) {
        this.showExif = showExif;
    }

    public int getImageIndex() {
        return imageIndex;
    }

    public void setImageIndex(int imageIndex) {
        this.imageIndex = imageIndex;
    }

    public HibInfoExtractDto getExtract() {
        return extract;
    }

    public void setExtract(HibInfoExtractDto extract) {
        this.extract = extract;
    }

    public String getMetaKey() {
        return metaKey;
    }

    public void setMetaKey(String metaKey) {
        this.metaKey = metaKey;
    }
}
