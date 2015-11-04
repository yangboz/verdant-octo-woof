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
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by yangboz on 11/1/15.
 */
public class HipiExample extends Configured implements Tool {

    private String[] args;

    public HipiExample(String[] args) {
        args = args;
        //Configuration testing
//        ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
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
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration, "HipiConfig");
        JobConf jobConf = new JobConf(configuration, Job.class);
//        job.getConfiguration().set(FileOutputFormat.);
        job.getConfiguration().set("mapreduce.input.fileinputformat.inputdir", strings[0]);
        job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", strings[1]);
        job.getConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        job.getConfiguration().set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        jobConf.setJobName("HipiJobConf");

        // Set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(HibInputFormat.class);
//        job.setInputFormat(HibInputFormat.class);
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
//        FileInputFormat.setInputPaths(jobConf, new Path(strings[0]));
        FileInputFormat.setInputPaths(jobConf, strings[0]);
        FileOutputFormat.setOutputPath(jobConf, new Path(strings[1]));
        //
        // Execute the MapReduce job and block until it complete
        boolean success = job.waitForCompletion(true);
        // Return success or failure
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] innerArgs = new String[]{"file:////Users/yangboz/SampleImages/SampleImages.hib", "file:////Users/yangboz/SampleImages/output/"};
        ToolRunner.run(new HipiExample(innerArgs), innerArgs);
        System.exit(0);
    }
}
