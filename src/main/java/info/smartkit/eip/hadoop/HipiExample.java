package info.smartkit.eip.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.hipi.image.FloatImage;
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
import org.hipi.imagebundle.mapreduce.HibInputFormat;

/**
 * Created by yangboz on 11/1/15.
 */
public class HipiExample extends Configured implements Tool {

    private String[] args;

    public HipiExample(String[] args) {
        args = args;
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("Hello HIPI,with args:" + strings[0]);
        // Check input arguments
        if (strings.length != 2) {
            System.out.println("Usage: helloWorld <input HIB>,<output directory>");
            System.exit(0);
        } else {
            System.out.println("Usage: helloWorld <input HIB>:" + strings[0] + "<output directory>:" + strings[1]);
        }
        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        JobConf jobConf = new JobConf(new Configuration(), Job.class);
        jobConf.setJobName("HipiJob");

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
        FileInputFormat.setInputPaths(jobConf, new Path(this.args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(this.args[1]));
        // Execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);
        // Return success or failure
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] innerArgs = new String[]{"~/SampleImages/", "~/SampleImages/output"};
        ToolRunner.run(new HipiExample(innerArgs), innerArgs);
        System.exit(0);
    }
}
