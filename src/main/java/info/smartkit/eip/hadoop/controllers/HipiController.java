package info.smartkit.eip.hadoop.controllers;

import com.wordnik.swagger.annotations.ApiOperation;
import info.smartkit.eip.hadoop.App;
import info.smartkit.eip.hadoop.dto.HibDumpDto;
import info.smartkit.eip.hadoop.dto.HibImportDto;
import info.smartkit.eip.hadoop.dto.HibInfoDto;
import info.smartkit.eip.hadoop.hipi.HibDumpMapper;
import info.smartkit.eip.hadoop.hipi.HibDumpReducer;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.hipi.image.*;
import org.hipi.imagebundle.HipiImageBundle;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import javax.validation.Valid;
import javax.ws.rs.core.MediaType;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by yangboz on 11/4/15.
 */
@RestController
@RequestMapping("/smartkit/hipi")
public class HipiController {
    // Autowire an object of type FaceInfoDao
//    @Autowired
//    private HipiItf _hipiService;

//    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON)
//    @ApiOperation(httpMethod = "POST", value = "Response a string describing if the face info is successfully created or not.")
//    public JsonObject create(@RequestBody @Valid FaceInfo faceInfo) {
//        return new JsonObject(_faceInfoDao.save(faceInfo));
//    }
//
//    @RequestMapping(method = RequestMethod.GET)
//    @ApiOperation(httpMethod = "GET", value = "Response a list describing all of face info that is successfully get or not.")
//    public JsonObject list() {
//        return new JsonObject(this._faceInfoDao.findAll());
//    }
//
//    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
//    @ApiOperation(httpMethod = "GET", value = "Response a string describing if the face info id is successfully get or not.")
//    public JsonObject get(@PathVariable("id") long id) {
//        return new JsonObject(this._faceInfoDao.findOne(id));
//    }

    //    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
//    @ApiOperation(httpMethod = "PUT", value = "Response a string describing if the  face info is successfully updated or not.")
//    public JsonObject update(@PathVariable("id") long id, @RequestBody @Valid FaceInfo faceInfo) {
////		FaceInfo find = this._faceInfoDao.findOne(id);
//        faceInfo.setId(id);
//        return new JsonObject(this._faceInfoDao.save(faceInfo));
//    }
//
//    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
//    @ApiOperation(httpMethod = "DELETE", value = "Response a string describing if the face info is successfully delete or not.")
//    public ResponseEntity<Boolean> delete(@PathVariable("id") long id) {
//        this._faceInfoDao.delete(id);
//        return new ResponseEntity<Boolean>(Boolean.TRUE, HttpStatus.OK);
//    }
    @RequestMapping(value = "import", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(httpMethod = "POST", value = "hibImport creates a HipiImageBundle (HIB) from a folder of images on your local file system."
            , notes = "A HIB is the key input file to the HIPI framework and represents a collection of images stored on the Hadoop Distributed File System (HDFS).")
    public List<String> hibImport(@RequestBody @Valid HibImportDto hibImportDto) throws IOException {
        List<String> results = new ArrayList<String>();
        //@see: https://github.com/uvagfx/hipi/blob/release/tools/hibImport/src/main/java/org/hipi/tools/HibImport.java
        String imageDir = hibImportDto.getInput();
        String outputHib = hibImportDto.getOutput();
        boolean overwrite = hibImportDto.getOverwrite();
        boolean hdfsInput = hibImportDto.getFormat().equals("hdfs") ? true : false;
//
        System.out.println("Input image directory: " + imageDir);
        System.out.println("Input FS: " + (hdfsInput ? "HDFS" : "local FS"));
        System.out.println("Output HIB: " + outputHib);
        System.out.println("Overwrite HIB if it exists: " + (overwrite ? "true" : "false"));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //
        if (hdfsInput) {

            FileStatus[] files = fs.listStatus(new Path(imageDir));
            if (files == null) {
                System.err.println(String.format("Did not find any files in the HDFS directory [%s]", imageDir));
                System.exit(0);
            }
            Arrays.sort(files);

            HipiImageBundle hib = new HipiImageBundle(new Path(outputHib), conf);
            hib.openForWrite(overwrite);

            for (FileStatus file : files) {
                FSDataInputStream fdis = fs.open(file.getPath());
                String source = file.getPath().toString();
                HashMap<String, String> metaData = new HashMap<String, String>();
                metaData.put("source", source);
                String fileName = file.getPath().getName().toLowerCase();
                System.out.println("HDFS fileName:" + fileName);
                if (-1 != fileName.lastIndexOf('.')) {
                    String suffix = fileName.substring(fileName.lastIndexOf('.'));
                    if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
                        hib.addImage(fdis, HipiImageHeader.HipiImageFormat.JPEG, metaData);
                        System.out.println(" ** added: " + fileName);
                        results.add(" ** added: " + fileName);
                    } else if (suffix.compareTo(".png") == 0) {
                        hib.addImage(fdis, HipiImageHeader.HipiImageFormat.PNG, metaData);
                        System.out.println(" ** added: " + fileName);
                        results.add(" ** added: " + fileName);
                    }
                }
            }

            hib.close();

        } else {

            File folder = new File(imageDir);
            File[] files = folder.listFiles();
            Arrays.sort(files);

            if (files == null) {
                System.err.println(String.format("Did not find any files in the local FS directory [%s]", imageDir));
                System.exit(0);
            }

            HipiImageBundle hib = new HipiImageBundle(new Path(outputHib), conf);
            hib.openForWrite(overwrite);

            for (File file : files) {
                FileInputStream fis = new FileInputStream(file);
                String localPath = file.getPath();
                HashMap<String, String> metaData = new HashMap<String, String>();
                metaData.put("source", localPath);
                String fileName = file.getName().toLowerCase();
                System.out.println("local FS fileName:" + fileName);
                if (-1 != fileName.lastIndexOf('.')) {
                    String suffix = fileName.substring(fileName.lastIndexOf('.'));
                    if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
                        hib.addImage(fis, HipiImageHeader.HipiImageFormat.JPEG, metaData);
                        System.out.println(" ** added: " + fileName);
                        results.add(" ** added: " + fileName);
                    } else if (suffix.compareTo(".png") == 0) {
                        hib.addImage(fis, HipiImageHeader.HipiImageFormat.PNG, metaData);
                        System.out.println(" ** added: " + fileName);
                        results.add(" ** added: " + fileName);
                    }
                }
            }

            hib.close();

        }

        System.out.println("Created: " + outputHib + " and " + outputHib + ".dat");
        results.add("Created: " + outputHib + " and " + outputHib + ".dat");
        return results;
    }

    @RequestMapping(value = "info", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(httpMethod = "POST", value = "The hibInfo tool allows querying basic information about HIBs such as image count, spatial dimensions of individual images, image meta data stored at the time of HIB creation, and image EXIF data."
            , notes = "It also allows extracting individual images as a stand-alone JPEG or PNG.")
    public List<String> hibInfo(@RequestBody @Valid HibInfoDto hibInfoDto) throws IOException {
        List<String> results = new ArrayList<String>();
        // Validate input HIB
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String inputHib = hibInfoDto.getInput();
        if (!fs.exists(new Path(inputHib))) {
            System.err.println("HIB index file not found: " + inputHib);
            System.exit(1);
        }
        if (!fs.exists(new Path(inputHib + ".dat"))) {
            System.err.println("HIB data file not found: " + inputHib + ".dat");
            System.exit(1);
        }
        //
        boolean showExif = hibInfoDto.getShowExif();
        boolean showMeta = hibInfoDto.getShowMeta();

        System.out.println("Input HIB: " + inputHib);
        System.out.println("Display meta data: " + (showMeta ? "true" : "false"));
        System.out.println("Display EXIF data: " + (showExif ? "true" : "false"));

        int imageIndex = -1;
        String extractImagePath = null;
        String metaKey = null;


        // try to decode image index
        try {
            imageIndex = hibInfoDto.getExtract().getIndex();
        } catch (NumberFormatException ex) {
            System.err.println("Unrecognized image index: " + imageIndex);
            usage();
        }

        extractImagePath = hibInfoDto.getExtract().getFileName();
        if (extractImagePath == null || extractImagePath.length() == 0) {
            usage();
        }

        metaKey = hibInfoDto.getMetaKey();
        if (metaKey == null || metaKey.length() == 0) {
            usage();
        }

        System.out.println("Image index: " + imageIndex);
        System.out.println("Extract image path: " + (extractImagePath == null ? "none" : extractImagePath));
        System.out.println("Meta data key: " + (metaKey == null ? "none" : metaKey));


        HipiImageBundle hib = null;
        try {
            hib = new HipiImageBundle(new Path(inputHib), new Configuration(), HipiImageFactory.getByteImageFactory());
            hib.openForRead((imageIndex == -1 ? 0 : imageIndex));
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }

        if (imageIndex == -1) {
            int count = 0;
            while (hib.next()) {
                System.out.println("IMAGE INDEX: " + count);
                results.add("IMAGE INDEX: " + count);
                HipiImageHeader header = hib.currentHeader();
                results.addAll(displayImageHeader(header, showMeta, showExif));
                count++;
            }
            if (imageIndex == -1) {
                System.out.println(String.format("Found [%d] images.", count));
            }
        } else {

            if (!hib.next()) {
                System.err.println(String.format("Failed to locate image with index [" + imageIndex + "]. Check that HIB contains sufficient number of images."));
                System.exit(0);
            }

            HipiImageHeader header = hib.currentHeader();
            results.addAll(displayImageHeader(header, showMeta, showExif));

            if (extractImagePath != null) {

                String imageExtension = FilenameUtils.getExtension(extractImagePath);
                if (imageExtension == null) {
                    System.err.println(String.format("Failed to determine image type based on extension [%s]. Please provide a valid path with complete extension.", extractImagePath));
                    System.exit(0);
                }

                ImageOutputStream ios = null;
                try {
                    ios = ImageIO.createImageOutputStream(new File(extractImagePath));
                } catch (IOException ex) {
                    System.err.println(String.format("Failed to open image file for writing [%s]", extractImagePath));
                    System.exit(0);
                }
                Iterator<ImageWriter> writers = ImageIO.getImageWritersBySuffix(imageExtension);
                if (writers == null) {
                    System.err.println(String.format("Failed to locate encoder for image extension [%s]", imageExtension));
                    System.exit(0);
                }
                ImageWriter writer = writers.next();
                if (writer == null) {
                    System.err.println(String.format("Failed to locate encoder for image extension [%s]", imageExtension));
                    System.exit(0);
                }
                System.out.println("Using image encoder: " + writer);
                results.add("Using image encoder: " + writer);
                writer.setOutput(ios);

                HipiImage image = hib.currentImage();

                int w = image.getWidth();
                int h = image.getHeight();

                BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

                PixelArray pa = ((RasterImage) image).getPixelArray();
                int[] rgb = new int[w * h];
                for (int i = 0; i < w * h; i++) {

                    int r = pa.getElemNonLinSRGB(i * 3 + 0);
                    int g = pa.getElemNonLinSRGB(i * 3 + 1);
                    int b = pa.getElemNonLinSRGB(i * 3 + 2);

                    rgb[i] = (r << 16) | (g << 8) | b;
                }
                bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);

                ImageWriteParam param = writer.getDefaultWriteParam();
                IIOImage iioImage = new IIOImage(bufferedImage, null, null);
                writer.write(null, iioImage, param);

                System.out.println(String.format("Wrote [%s]", extractImagePath));
                results.add(String.format("Wrote [%s]", extractImagePath));
            }

            if (metaKey != null) {
                String metaValue = header.getMetaData(metaKey);
                if (metaValue == null) {
                    System.out.println("Meta data key [" + metaKey + "] not found.");
                } else {
                    System.out.println(metaKey + ": " + metaValue);
                    results.add(metaKey + ": " + metaValue);
                }
            }

        }

        hib.close();
        //
        return results;
    }

    @RequestMapping(value = "dump", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(httpMethod = "POST", value = "Similar to hibInfo, but this is a MapReduce/program that extracts basic information about the images in a HIB."
            , notes = " It does this using multiple parallel map tasks (one mapper for each image in the HIB) and writes this information to a text file on the HDFS in a single reduce task.")
    public ResponseEntity<Boolean> hibDump(@RequestBody @Valid HibDumpDto hibDumpDto) throws IOException, ClassNotFoundException, InterruptedException {
        //
        List<String> results = new ArrayList<String>();
        //
        Configuration conf = new Configuration();//new Configuration();
        Job job = Job.getInstance(conf, "hibDump");
        job.setJarByClass(App.class);
        job.setMapperClass(HibDumpMapper.class);
        job.setReducerClass(HibDumpReducer.class);
        //
        job.setInputFormatClass(HibInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        //
        String inputPath = hibDumpDto.getInput();
        String outputPath = hibDumpDto.getOutput();

        removeDir(outputPath, conf);
        //
        JobConf jobConf = new JobConf(conf, Job.class);
//        job.getConfiguration().set(FileOutputFormat.);
        job.getConfiguration().set("mapreduce.input.fileinputformat.inputdir", inputPath);
        job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", outputPath);
        job.getConfiguration().set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        job.getConfiguration().set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        jobConf.setJobName("HipiJobConf");
        //
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? new ResponseEntity<Boolean>(Boolean.FALSE, HttpStatus.EXPECTATION_FAILED) : new ResponseEntity<Boolean>(Boolean.TRUE, HttpStatus.OK);
    }

    @RequestMapping(value = "download/{id}", method = RequestMethod.GET)
    @ApiOperation(httpMethod = "GET", value = "This is a MapReduce/HIPI program that creates a HIB from a set of images located on the Internet."
            , notes = " This program highlights some of the more subtle parts of HIPI and the Hadoop framework and will be a valuable tool for creating inputs. It is also designed to work seamlessly with the Yahoo/Flickr 100M Creative Commons research dataset.")
    public void hibDownload(@PathVariable("id") long id) {
        //TODO:
    }

    @RequestMapping(value = "toJpeg/{id}", method = RequestMethod.GET)
    @ApiOperation(httpMethod = "GET", value = "This is a MapReduce/HIPI program that extracts the images within a HIB as individual JPEG files written to the HDFS."
            , notes = " This program illustrates many important features of the HIPI API (e.g., how to process the images in a HIB according to the MapReduce programming model). It is also a useful tool to verify that a HIB has been properly created.")
    public void hibToJpeg(@PathVariable("id") long id) {
        //TODO:
    }

    @RequestMapping(value = "covar/{id}", method = RequestMethod.GET)
    @ApiOperation(httpMethod = "GET", value = "This is a MapReduce/HIPI program that implements the experiment described in the paper The Principal Components of Natural Images, written by Hancock et al. in 1992."
            , notes = " This program computes the principal components of natural image patches (eigenvectors of the covariance matrix computed over a large set of small image patches). This is a good starting point for learning how to build more complex HIPI programs and also illustrates HIPI's ability to interface with OpenCV.")
    public void covar(@PathVariable("id") long id) {
        //TODO:
    }


    ////
    private void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(148);
        formatter.printHelp("hibInfo.jar <input HIB> [--show-exif] [--show-meta] [#index [--extract file.png] [--meta key]]", null);
        System.exit(0);
    }

    private List<String> displayImageHeader(HipiImageHeader header, boolean showMeta, boolean showExif) {
        List<String> results = new ArrayList<>();

        System.out.println(String.format("   %d x %d", header.getWidth(), header.getHeight()));
        results.add(String.format("   %d x %d", header.getWidth(), header.getHeight()));
        System.out.println(String.format("   format: %d", header.getStorageFormat().toInteger()));
        results.add(String.format("   format: %d", header.getStorageFormat().toInteger()));

        if (showMeta) {
            HashMap<String, String> metaData = header.getAllMetaData();
            System.out.println("   meta: " + metaData);
            results.add("   meta: " + metaData);
        }

        if (showExif) {
            HashMap<String, String> exifData = header.getAllExifData();
            System.out.println("   exif: " + exifData);
            results.add("   exif: " + exifData);
        }

        return results;
    }

    private void removeDir(String path, Configuration conf) throws IOException {
        Path output_path = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output_path)) {
            fs.delete(output_path, true);
        }
    }

}
