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

    }
}
