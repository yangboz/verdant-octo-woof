package info.smartkit.eip.hadoop.controllers;

import com.wordnik.swagger.annotations.ApiOperation;
import info.smartkit.eip.hadoop.dto.HibImportDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.HipiImageBundle;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

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
    public String hibImport(@RequestBody @Valid HibImportDto hibImportDto) throws IOException {
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
                    } else if (suffix.compareTo(".png") == 0) {
                        hib.addImage(fdis, HipiImageHeader.HipiImageFormat.PNG, metaData);
                        System.out.println(" ** added: " + fileName);
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
                    } else if (suffix.compareTo(".png") == 0) {
                        hib.addImage(fis, HipiImageHeader.HipiImageFormat.PNG, metaData);
                        System.out.println(" ** added: " + fileName);
                    }
                }
            }

            hib.close();

        }

        System.out.println("Created: " + outputHib + " and " + outputHib + ".dat");
        return "Created: " + outputHib + " and " + outputHib + ".dat";
    }

    @RequestMapping(value = "info/{id}", method = RequestMethod.GET)
    @ApiOperation(httpMethod = "GET", value = "The hibInfo tool allows querying basic information about HIBs such as image count, spatial dimensions of individual images, image meta data stored at the time of HIB creation, and image EXIF data. It also allows extracting individual images as a stand-alone JPEG or PNG.")
    public void hibInfo(@PathVariable("id") long id) {
        //TODO:
    }

    @RequestMapping(value = "dump/{id}", method = RequestMethod.GET)
    @ApiOperation(httpMethod = "GET", value = "Similar to hibInfo, but this is a MapReduce/program that extracts basic information about the images in a HIB."
            , notes = " It does this using multiple parallel map tasks (one mapper for each image in the HIB) and writes this information to a text file on the HDFS in a single reduce task.")
    public void hibDump(@PathVariable("id") long id) {
        //TODO:
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
}
