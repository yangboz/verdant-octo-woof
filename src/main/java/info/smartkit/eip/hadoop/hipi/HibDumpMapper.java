package info.smartkit.eip.hadoop.hipi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.ByteImage;
import org.hipi.image.HipiImageHeader;

import java.io.IOException;

/**
 * Created by yangboz on 11/6/15.
 */
public class HibDumpMapper extends Mapper<HipiImageHeader, ByteImage, IntWritable, Text> {
    @Override
    public void map(HipiImageHeader header, ByteImage image, Context context) throws IOException, InterruptedException {

        String output = null;

        if (header == null) {
            output = "Failed to read image header.";
        } else if (image == null) {
            output = "Failed to decode image data.";
        } else {
            int w = header.getWidth();
            int h = header.getHeight();
            String source = header.getMetaData("source");
            String cameraModel = header.getExifData("Model");
            output = w + "x" + h + "\t(" + source + ")\t  " + cameraModel;
        }

        context.write(new IntWritable(1), new Text(output));
    }
}
