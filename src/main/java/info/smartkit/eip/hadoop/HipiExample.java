package info.smartkit.eip.hadoop;

import hipi.image.FloatImage;
import info.smartkit.eip.hadoop.hipi.HelloWorldMapper;
import info.smartkit.eip.hadoop.hipi.HelloWorldReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yangboz on 11/1/15.
 */
public class HipiExample extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("Hello HIPI!");
        // Check input arguments
        if (strings.length != 2) {
            System.out.println("Usage: helloWorld <input HIB> <output directory>");
            System.exit(0);
        }
        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(HibInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(HipiExample.class);
        job.setMapperClass(HelloWorldMapper.class);
        job.setReducerClass(HelloWorldReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatImage.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // Set the input and output paths on the HDFS
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        // Execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);
        // Return success or failure
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HipiExample(), args);
        System.exit(0);
    }
}
