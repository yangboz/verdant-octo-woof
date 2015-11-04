package info.smartkit.eip.hadoop.hipi;

import org.hipi.image.FloatImage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.HipiImageHeader;

import java.io.IOException;

/**
 * Created by yangboz on 11/1/15.
 */
public class HelloWorldMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, FloatImage> {
    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException {
        // Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
        if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {

            // Get dimensions of image
            int w = value.getWidth();
            int h = value.getHeight();

            // Get pointer to image data
            float[] valData = value.getData();

            // Initialize 3 element array to hold RGB pixel average
            float[] avgData = {0, 0, 0};

            // Traverse image pixel data in raster-scan order and update running average
            for (int j = 0; j < h; j++) {
                for (int i = 0; i < w; i++) {
                    avgData[0] += valData[(j * w + i) * 3 + 0]; // R
                    avgData[1] += valData[(j * w + i) * 3 + 1]; // G
                    avgData[2] += valData[(j * w + i) * 3 + 2]; // B
                }
            }

            // Create a FloatImage to store the average value
            FloatImage avg = new FloatImage(1, 1, 3, avgData);

            // Divide by number of pixels in image
            avg.scale(1.0f / (float) (w * h));

            // Emit record to reducer
            context.write(new IntWritable(1), avg);

        } // If (value != null...
    }

}
