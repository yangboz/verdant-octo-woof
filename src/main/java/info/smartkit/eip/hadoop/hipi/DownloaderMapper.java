package info.smartkit.eip.hadoop.hipi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.io.JpegCodec;
import org.hipi.image.io.PngCodec;
import org.hipi.imagebundle.HipiImageBundle;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by yangboz on 11/7/15.
 */
public class DownloaderMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {


    private long uniqueMapperKey = 0; // Ensures temp hib paths in mapper are unique
    private long numDownloads = 0; // Keeps track of number of image downloads

    private Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Use line number and a unique key assigned to each map task to generate a unique filename.
        String tempPath = conf.get("downloader.outpath") + key.get() + uniqueMapperKey + ".hib.tmp";

        boolean yfcc100m = conf.getBoolean("downloader.yfcc100m", false);

        // Create new temporary HIB
        HipiImageBundle hib = new HipiImageBundle(new Path(tempPath), conf);
        hib.openForWrite(true);

        // The value argument contains a list of image URLs delimited by
        // '\n'. Setup buffered reader to allow processing this string
        // line by line.
        BufferedReader lineReader = new BufferedReader(new StringReader(value.toString()));
        String line;

        // Iterate through URLs
        while ((line = lineReader.readLine()) != null) {

            String[] lineFields = null;
            String imageUri = null;

            if (yfcc100m) {
                // Split line into fields
                lineFields = line.split("\t"); // Fields within each line are delimited by tabs
                if (lineFields[22].equals("1")) { // 0 = image, 1 = video in YFCC100M format
                    continue;
                }
                imageUri = lineFields[14];
            } else {
                imageUri = line; // Otherwise, assume entire line is image URL
            }

            long startTime = System.currentTimeMillis();
            try {

                String type = "";
                URLConnection conn;

                // Attempt to download image at URL using java.net
                try {
                    URL link = new URL(imageUri);
                    numDownloads++;
                    System.out.println("");
                    System.out.println("Downloading: " + link.toString());
                    System.out.println("Number of downloads: " + numDownloads);
                    conn = link.openConnection();
                    conn.connect();
                    type = conn.getContentType();

                    // Check that image format is supported, header is parsable, and add to HIB if so
                    if (type != null && (type.compareTo("image/jpeg") == 0 || type.compareTo("image/png") == 0)) {

                        // Get input stream for URL connection
                        InputStream bis = new BufferedInputStream(conn.getInputStream());

                        // Mark current location in stream for later reset
                        bis.mark(Integer.MAX_VALUE);

                        // Attempt to decode the image header
                        HipiImageHeader header = (type.compareTo("image/jpeg") == 0 ?
                                JpegCodec.getInstance().decodeHeader(bis) :
                                PngCodec.getInstance().decodeHeader(bis));

                        if (header == null) {
                            System.out.println("Failed to parse header, image not added to HIB: " + link.toString());
                        } else {

                            // Passed header decode test, so reset to beginning of stream
                            bis.reset();

                            if (yfcc100m) {
                                // Capture fields as image metadata for posterity
                                for (int i = 0; i < lineFields.length; i++) {
                                    header.addMetaData(String.format("col_%03d", i), lineFields[i]);
                                }
                                header.addMetaData("source", lineFields[14]);
                            } else {
                                // Capture source URL as image metadata for posterity
                                header.addMetaData("source", imageUri);
                            }

                            // Add image to hib
                            hib.addImage(header, bis);

                            System.err.println("Added to HIB: " + imageUri);
                        }
                    } else {
                        System.out.println("Unrecognized HTTP content type or unsupported image format [" + type + "], not added to HIB: " + imageUri);
                    }
                } catch (Exception e) {
                    System.out.println("Connection error while trying to download: " + imageUri);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                System.out.println("Network error while trying to download: " + imageUri);
                e.printStackTrace();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }

            float el = (float) (System.currentTimeMillis() - startTime) / 1000.0f;
            System.out.println("> Time elapsed: " + el + " seconds");

        } // while ((line = lineReader.readLine()) != null) {

        try {
            // Output key/value pair to reduce layer consisting of boolean and path to HIB
            context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
            // Cleanup
            lineReader.close();
            hib.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        uniqueMapperKey++;

    }
}
