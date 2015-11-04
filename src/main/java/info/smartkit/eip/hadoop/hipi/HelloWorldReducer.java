package info.smartkit.eip.hadoop.hipi;

import org.hipi.image.FloatImage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangboz on 11/1/15.
 */
public class HelloWorldReducer extends Reducer<IntWritable, FloatImage, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<FloatImage> values, Context context) throws IOException, InterruptedException {
        // Create FloatImage object to hold final result
        FloatImage avg = new FloatImage(1, 1, 3);

        // Initialize a counter and iterate over IntWritable/FloatImage records from mapper
        int total = 0;
        for (FloatImage val : values) {
            avg.add(val);
            total++;
        }

        if (total > 0) {
            // Normalize sum to obtain average
            avg.scale(1.0f / total);
            // Assemble final output as string
            float[] avgData = avg.getData();
            String result = String.format("Average pixel value: %f %f %f", avgData[0], avgData[1], avgData[2]);
            // Emit output of job which will be written to HDFS
            context.write(key, new Text(result));
        }
    }
}
