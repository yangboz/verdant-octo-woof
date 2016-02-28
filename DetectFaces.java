import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.opencv.highgui.*;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.objdetect.CascadeClassifier;


import java.io.File;


/**
* Created by dmalav on 4/30/15.
*/
public class DetectFaces {


   public void run(String imageFile) {
       System.out.println("\nRunning DetectFaceDemo");


       // Create a face detector from the cascade file in the resources
       // directory.
       String xmlPath = "/home/cloudera/project/opencv-examples/lbpcascade_frontalface.xml";
       System.out.println(xmlPath);
       CascadeClassifier faceDetector = new CascadeClassifier(xmlPath);
       Mat image = Highgui.imread(imageFile);


       // Detect faces in the image.
       // MatOfRect is a special container class for Rect.
       MatOfRect faceDetections = new MatOfRect();
       faceDetector.detectMultiScale(image, faceDetections);


       System.out.println(String.format("Detected %s faces", faceDetections.toArray().length));


       // Draw a bounding box around each face.
       for (Rect rect : faceDetections.toArray()) {
           Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0));
       }


       File f = new File(imageFile);
       System.out.println(f.getName());
       // Save the visualized detection.
       String filename = f.getName();
       System.out.println(String.format("Writing %s", filename));
       Highgui.imwrite(filename, image);


   }
}

