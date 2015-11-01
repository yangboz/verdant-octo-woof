package info.smartkit.eip.hadoop.hipi;

import hipi.image.FloatImage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangboz on 11/1/15.
 */
public class HelloWorldMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, FloatImage> {
    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException {

    }

}
