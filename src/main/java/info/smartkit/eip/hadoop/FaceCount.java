package info.smartkit.eip.hadoop;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
//import org.hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import org.hipi.imagebundle.mapreduce.HibInputFormat;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.opencv.core.*;
import org.opencv.objdetect.CascadeClassifier;


import java.io.IOException;
import java.net.URI;


public class FaceCount extends Configured implements Tool {


//    public static class FaceCountMapper extends Mapper<ImageHeader, FloatImage, IntWritable, IntWritable> {
    public static class FaceCountMapper extends Mapper<HipiImageHeader, FloatImage, IntWritable, IntWritable> {


        // Create a face detector from the cascade file in the resources
        // directory.
        private CascadeClassifier faceDetector;


        // Convert HIPI FloatImage to OpenCV Mat
        public Mat convertFloatImageToOpenCVMat(FloatImage floatImage) {


            // Get dimensions of image
            int w = floatImage.getWidth();
            int h = floatImage.getHeight();


            // Get pointer to image data
            float[] valData = floatImage.getData();


            // Initialize 3 element array to hold RGB pixel average
            double[] rgb = {0.0,0.0,0.0};


            Mat mat = new Mat(h, w, CvType.CV_8UC3);


            // Traverse image pixel data in raster-scan order and update running average
            for (int j = 0; j < h; j++) {
                for (int i = 0; i < w; i++) {
                    rgb[0] = (double) valData[(j*w+i)*3+0] * 255.0; // R
                    rgb[1] = (double) valData[(j*w+i)*3+1] * 255.0; // G
                    rgb[2] = (double) valData[(j*w+i)*3+2] * 255.0; // B
                    mat.put(j, i, rgb);
                }
            }


            return mat;
        }


        // Count faces in image
        public int countFaces(Mat image) {


            // Detect faces in the image.
            // MatOfRect is a special container class for Rect.
            MatOfRect faceDetections = new MatOfRect();
            faceDetector.detectMultiScale(image, faceDetections);


            return faceDetections.toArray().length;
        }


        public void setup(Context context)
                throws IOException, InterruptedException {


            // Load OpenCV native library
            try {
                System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
            } catch (UnsatisfiedLinkError e) {
                System.err.println("Native code library failed to load.\n" + e + Core.NATIVE_LIBRARY_NAME);
                System.exit(1);
            }


            // Load cached cascade file for front face detection and create CascadeClassifier
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                URI mappingFileUri = context.getCacheFiles()[0];


                if (mappingFileUri != null) {
                    faceDetector = new CascadeClassifier("./lbpcascade_frontalface.xml");


                } else {
                    System.out.println(">>>>>> NO MAPPING FILE");
                }
            } else {
                System.out.println(">>>>>> NO CACHE FILES AT ALL");
            }


            super.setup(context);
        } // setup()


        public void map(HipiImageHeader key, FloatImage value, Context context)
                throws IOException, InterruptedException {


            // Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
//            i f (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getBands() == 3) {
                if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {


                Mat cvImage = this.convertFloatImageToOpenCVMat(value);


                int faces = this.countFaces(cvImage);


                System.out.println(">>>>>> Detected Faces: " + Integer.toString(faces));


                // Emit record to reducer
                context.write(new IntWritable(1), new IntWritable(faces));


            } // If (value != null...


        } // map()
    }


    public static class FaceCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {


            // Initialize a counter and iterate over IntWritable/FloatImage records from mapper
            int total = 0;
            int images = 0;
            for (IntWritable val : values) {
                total += val.get();
                images++;
            }


            String result = String.format("Total face detected: %d", total);
            // Emit output of job which will be written to HDFS
            context.write(new IntWritable(images), new Text(result));
        } // reduce()
    }


    public int run(String[] args) throws Exception {
        // Check input arguments
        if (args.length != 2) {
            System.out.println("Usage: firstprog <input HIB> <output directory>");
            System.exit(0);
        }


        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(HibInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(FaceCount.class);
        job.setMapperClass(FaceCountMapper.class);
        job.setReducerClass(FaceCountReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        // Set the input and output paths on the HDFS
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // add cascade file
        job.addCacheFile(new URI("/user/cloudera/lbpcascade_frontalface.xml#lbpcascade_frontalface.xml"));


        // Execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);


        // Return success or failure
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {


        ToolRunner.run(new FaceCount(), args);
        System.exit(0);
    }


}
